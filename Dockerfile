# syntax=docker/dockerfile:1
# Airflow 2.9.3 — first line with official Python 3.12 image support.
FROM apache/airflow:2.9.3-python3.12

# Only OUR extra dependencies — Airflow core + postgres provider already
# ship in the base image (that's how the official quick-start compose
# talks to a Postgres metadata DB with no custom build at all).
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# dags/, src/, tests/ are bind-mounted at runtime (see docker-compose.yml
# and ADR-003), so nothing else is COPYed here for local dev.
