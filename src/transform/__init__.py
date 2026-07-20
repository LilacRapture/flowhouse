"""
Transform logic — pure functions over pandas DataFrames (Phase 1), with a
PySpark variant added later (Phase 3, `spark_ops.py`) once the pandas
pipeline works end-to-end. Kept separate from `dags/` so it's unit-testable
without spinning up Airflow.

IMPORTANT: read extract's parquet output with
`pd.read_parquet(path, dtype_backend="pyarrow")` — not "numpy_nullable".
Both the default backend and "numpy_nullable" upcast int64 child fields
inside nested struct columns (owner, project) to float64 whenever any row
has a null struct, the same class of bug ADR-006 avoided at write time,
resurfacing one level deeper at read time. Verified empirically; see
ADR-009. Only "pyarrow" preserves nested int64 fields correctly.
"""
