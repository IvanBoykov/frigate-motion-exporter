ARG BASE_IMAGE=python:3.14-slim

FROM ${BASE_IMAGE} AS deps

# Copy only the dependency file so the Docker layer cache is reusable.
COPY requirements.txt .
# Install dependencies into an isolated prefix.
ENV PIP_NO_CACHE_DIR=1 \
      PIP_NO_COMPILE=1 \
      PIP_DISABLE_PIP_VERSION_CHECK=1 \
      PYTHONDONTWRITEBYTECODE=1 \
      PYTHONHASHSEED=0 \
      LC_ALL=C

RUN pip install --no-cache-dir --require-hashes --no-compile --prefix=/install -r requirements.txt

# Final stage (runtime)
FROM ${BASE_IMAGE}

WORKDIR /app

# Copy the installed dependencies from the deps stage.
# The /install prefix lands cleanly in /usr/local,
# which is already on PATH and PYTHONPATH in the base image.
COPY --from=deps /install /usr/local
ENV PYTHONUNBUFFERED=1
# Run as an unprivileged user.
RUN useradd -m -r appuser
# Copy the application code; src/ becomes the working directory, so the
# docker runs frigate_s3_archiver.py as a top-level module.
COPY --chown=appuser:appuser src/ ./

USER appuser

# Command
CMD ["python", "frigate_s3_archiver.py"]
EXPOSE 9108
