"""
Loaders — write transformed data into ClickHouse.

Reload strategy: per-day partition refresh, not whole-table truncate.
daily_task_snapshot is partitioned by snapshot_date (one partition per
day); a reload for a given day drops just that day's partition before
inserting, leaving other days untouched — see clickhouse_loader.py and
ADR-011. This supersedes the original "full refresh (truncate + insert)"
placeholder plan, which predated ADR-008's snapshot-accumulates-forward
decision and would have deleted all prior history on every run.
"""
