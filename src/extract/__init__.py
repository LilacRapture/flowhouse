"""
Extractors — one module per data source.

`tasktracker.py` — authenticates against TaskTracker's /api/auth/login/
via the `tasktracker_api` Airflow Connection (ADR-005), pages through
/api/tasks/ and /api/projects/, writes raw records to parquet on the
shared volume for the transform step to pick up.
"""
