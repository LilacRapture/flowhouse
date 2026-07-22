# syntax=docker/dockerfile:1
# Airflow 2.9.3 — first line with official Python 3.12 image support.
FROM apache/airflow:2.9.3-python3.12

# Only OUR extra dependencies — Airflow core + postgres provider already
# ship in the base image (that's how the official quick-start compose
# talks to a Postgres metadata DB with no custom build at all).
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Pre-create the data volume mount path with correct ownership before the
# named volume (airflow_data, see docker-compose.yml) is ever attached —
# Docker copies an image's existing content+ownership into a freshly
# created named volume on first mount, so this avoids the airflow user
# (non-root) hitting PermissionError on os.makedirs() at runtime.
RUN mkdir -p /opt/airflow/data/raw
