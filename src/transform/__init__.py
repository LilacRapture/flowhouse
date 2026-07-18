"""
Transform logic — pure functions over pandas DataFrames (Phase 1), with a
PySpark variant added later (Phase 3, `spark_ops.py`) once the pandas
pipeline works end-to-end. Kept separate from `dags/` so it's unit-testable
without spinning up Airflow.
"""
