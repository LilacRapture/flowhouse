"""
Loaders — write transformed data into ClickHouse.

Two tables, two reload strategies (see clickhouse_loader.py docstring
for details):
- raw_tasks: whole-table TRUNCATE + insert, mirrors current TaskTracker
  state only (variant A — no history).
- daily_task_snapshot: per-day partition refresh, accumulates forward
  (ADR-008, ADR-011) — never a whole-table truncate.
"""
