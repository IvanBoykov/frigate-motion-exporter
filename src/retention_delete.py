"""Executing a retention plan against S3.

S3 has no delete-by-prefix: a folder is not an object, only a shared key
substring. So the plan's hour prefixes are a selection, not an argument - each
one is re-listed here and only keys that survive the re-check are deleted:

- the fresh listing still returns it (the census is minutes old and the upload
  path is still writing);
- it parses as one of our keys under our prefix - foreign bytes are measured
  and paid for, never deleted.

Each camera's newest hour folder never enters a plan in the first place
(:func:`retention.build_plan`), which is what keeps the camera's newest key -
the object the watermark is rebuilt from - safe, and keeps every camera
non-empty by construction.

Deletion is idempotent: a key that is already gone is not an error. A pass
killed halfway therefore needs no progress record - the next pass re-lists and
re-plans from the bucket, and deletes strictly less.
"""
import asyncio
import logging

from botocore.exceptions import ClientError

import archive_census
import frigate_s3_archiver as archiver

log = logging.getLogger("retention_delete")

DELETE_BATCH_KEYS = 1000

# Endpoints without the batch API answer the POST with a permanent-looking
# client error naming the missing feature; only those fall back to per-key
# deletion. Anything else (AccessDenied, SlowDown, a transport fault) is a
# real S3 condition and aborts the pass instead of hammering the endpoint with
# a thousand single deletes.
_BATCH_UNSUPPORTED_CODES = frozenset(
    {"NotImplemented", "MethodNotAllowed", "NotSupported"}
)


def _s3_error_code(exc):
    try:
        return str(exc.response["Error"]["Code"])
    except (AttributeError, KeyError, TypeError):
        return ""


def _recheck_folder(
    s3_client,
    bucket,
    folder_key,
    prefix,
    pattern,
    key_timezone,
):
    """Re-list one planned hour folder; return ``(keys, bytes)`` to delete.

    Only keys the fresh listing still shows and that parse as ours are kept.
    A listing error propagates: a folder we cannot re-read is a folder we do
    not delete.
    """
    keys = []
    total_bytes = 0
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=folder_key):
        for obj in page.get("Contents", []):
            key = obj.get("Key", "")
            kind, _camera, _folder, _epoch = archive_census.classify(
                key, prefix, pattern, key_timezone
            )
            if kind is not archive_census.OURS:
                continue
            keys.append(key)
            total_bytes += int(obj.get("Size", 0) or 0)
    return keys, total_bytes


def _delete_batch(s3_client, bucket, keys):
    """Delete up to ``DELETE_BATCH_KEYS`` keys; return the failed key names.

    ``DeleteObjects`` answers HTTP 200 with a per-key ``Errors`` list, so
    "no exception" is not "all gone" and the list has to be read. A response
    that claims a key failed is retried once per key - the per-key call is the
    only shape that separates a genuinely missing object from a batch fault.
    """
    response = s3_client.delete_objects(
        Bucket=bucket,
        Delete={
            "Objects": [{"Key": key} for key in keys],
            "Quiet": True,
        },
    )
    errors = response.get("Errors") or []
    return [str(err.get("Key", "")) for err in errors]


def _delete_batch_one_by_one(s3_client, bucket, keys):
    failed = []
    for key in keys:
        try:
            s3_client.delete_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            log.warning(
                "Delete of %s failed: %s", key, _s3_error_code(exc) or exc
            )
            failed.append(key)
    return failed


def _delete_keys(s3_client, bucket, keys, state):
    """One batch, with the batch-unsupported fallback recorded on ``state``.

    ``state`` is the per-call dict the caller threads through all batches so
    the endpoint's batch support is probed once, not re-probed per folder.
    """
    if not state["batch_supported"]:
        return _delete_batch_one_by_one(s3_client, bucket, keys)
    try:
        return _delete_batch(s3_client, bucket, keys)
    except ClientError as exc:
        if _s3_error_code(exc) not in _BATCH_UNSUPPORTED_CODES:
            raise
        log.warning(
            "Endpoint does not support batch delete (%s); falling back to"
            " per-key deletion for the rest of the pass",
            _s3_error_code(exc),
        )
        state["batch_supported"] = False
        return _delete_batch_one_by_one(s3_client, bucket, keys)


def execute_plan(
    s3_client,
    bucket,
    plan,
    prefix="",
    key_timezone=None,
    cancel_event=None,
):
    """Delete the plan's folders; return per-camera ``(keys, bytes)`` and the
    list of keys any delete call reported as failed.

    Runs in a worker thread like every other blocking S3 path. ``cancel_event``
    is checked at every folder and batch boundary: ``asyncio.to_thread`` cannot
    interrupt a thread, so a cancelled prune pass would otherwise keep deleting
    after its task was detached (see ``_check_cancelled`` in the uploader for
    the same mechanism).
    """
    if key_timezone is None:
        key_timezone = archiver.DEFAULT_S3_KEY_TIMEZONE

    pattern = archive_census.build_pattern(key_timezone)
    state = {"batch_supported": True}
    per_camera = {}
    failed_keys = []
    deleted_bytes = 0

    for folder in plan.folders:
        if cancel_event is not None and cancel_event.is_set():
            raise asyncio.CancelledError(
                f"retention deletion stopped at {folder.folder_key}"
            )

        keys, folder_bytes = _recheck_folder(
            s3_client,
            bucket,
            folder.folder_key,
            prefix,
            pattern,
            key_timezone,
        )
        if not keys:
            continue

        deleted_here = 0
        for offset in range(0, len(keys), DELETE_BATCH_KEYS):
            if cancel_event is not None and cancel_event.is_set():
                raise asyncio.CancelledError(
                    f"retention deletion stopped at {folder.folder_key}"
                )
            batch = keys[offset : offset + DELETE_BATCH_KEYS]
            failed = _delete_keys(s3_client, bucket, batch, state)
            deleted_here += len(batch) - len(failed)
            failed_keys.extend(failed)

        if deleted_here:
            # Charge the freed bytes from the fresh re-listing, scaled by what
            # actually deleted: keys can vanish (or arrive) between the census
            # and now, and the counters should describe the deletes, not the
            # minutes-old plan.
            freed = (
                folder_bytes * deleted_here // len(keys)
                if len(keys)
                else 0
            )
            deleted_bytes += freed
            entry = per_camera.setdefault(folder.camera, [0, 0])
            entry[0] += deleted_here
            entry[1] += freed

    return per_camera, deleted_bytes, failed_keys
