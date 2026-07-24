# syntax=docker/dockerfile:1
# Airflow 2.9.3 — first line with official Python 3.12 image support.
FROM apache/airflow:2.9.3-python3.12

# PySpark needs a JRE. The base image runs as the
# non-root `airflow` user by default —
# switch to root for apt-get, then back, mirroring the base image's own
# convention rather than adding a custom entrypoint.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*
# Verified via: docker compose exec airflow readlink -f $(which java)
# Architecture-specific path (arm64) — if rebuilt on amd64, 
# this will need updating to .../java-17-
# openjdk-amd64. For CI: ADR-015 has CI install Airflow via
# pip directly, never building this Dockerfile.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
USER airflow

# Only OUR extra dependencies: Airflow core + postgres provider already
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
