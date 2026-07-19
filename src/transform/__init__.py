"""
Transform logic — pure functions over pandas DataFrames (Phase 1), with a
PySpark variant added later (Phase 3, `spark_ops.py`) once the pandas
pipeline works end-to-end. Kept separate from `dags/` so it's unit-testable
without spinning up Airflow.

IMPORTANT: read extract's parquet output with
`pd.read_parquet(path, dtype_backend="numpy_nullable")` — the plain
default call upcasts nullable int columns to float64 on read, silently
undoing the reason extract writes via pyarrow directly instead of
pandas.to_parquet() (see ADR-006).
"""
