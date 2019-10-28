# docker build -t gcs-file-ingestor .
FROM python:3.7

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
# At run time, /data must be binded to a volume containing a valid Service Account credentials file
# named gcs-file-ingestor-credentials.json.
ENV GOOGLE_APPLICATION_CREDENTIALS=/data/datacatalog-fileset-enricher.json

WORKDIR /app

# Copy project files (see .dockerignore).
COPY . .

# Install gcs-file-creator package from source files.
RUN pip install .

ENTRYPOINT ["python", "main.py"]
