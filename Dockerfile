FROM python:3.14-slim AS deps

WORKDIR /build

# Copy only the dependency file so the Docker layer cache is reusable.
COPY requirements.txt .

# Install dependencies into an isolated prefix.
# --no-cache-dir keeps the layer smaller.
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Final stage (runtime)
FROM python:3.14-slim

WORKDIR /app

# Copy the installed dependencies from the deps stage.
# The /install prefix lands cleanly in /usr/local,
# which is already on PATH and PYTHONPATH in the base image.
COPY --from=deps /install /usr/local
ENV PYTHONUNBUFFERED=1
# Run as an unprivileged user.
RUN useradd -m -r appuser
# Copy the application code; src/ becomes the working directory, so the
# entrypoint runs frigate_s3_archiver.py as a top-level module.
COPY --chown=appuser:appuser src/ ./

USER appuser

# Entrypoint
ENTRYPOINT ["python", "frigate_s3_archiver.py"]
