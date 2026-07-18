"""
Loaders — write transformed data into ClickHouse.

Phase 1 next step: `clickhouse_loader.py`, using clickhouse-connect.
MVP strategy is full refresh (truncate + insert) into `daily_task_stats`;
incremental loads via `updated_at` are deferred (see docs/decisions.md).
"""
