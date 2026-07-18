"""
Extractors — one module per data source.

Phase 1 next step: `tasktracker.py` — authenticates against
/api/auth/login/ (credentials via an Airflow Connection, not hardcoded),
then pages through /api/tasks/ and /api/projects/ and writes raw records
to parquet on the shared volume for the transform step to pick up.

Phase 4 (later, pending source format): `fairytale.py`.
"""
