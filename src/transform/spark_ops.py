"""
PySpark variant of the transform step.

Mirrors pandas_ops.py's two functions — build_daily_task_snapshot() and
build_raw_tasks() — with the same grouping logic, the same overdue
definition, and the same output column set/order, so the DAG can select
either engine for either function without the load step caring which one
ran (src/load/clickhouse_loader.py only ever sees a pandas DataFrame,
produced here via .toPandas() as the final step).

Runs as a single local SparkSession (local[2]) inside the same Airflow
container as pandas_ops — no separate Spark cluster (ADR-017).
"""
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger(__name__)

_NO_PROJECT_ID = 0
_NO_PROJECT_NAME = "(no project)"

_SNAPSHOT_OUTPUT_COLUMNS = [
    "snapshot_date", "project_id", "project_name", "owner_id", "owner_email",
    "status", "task_count", "overdue_count",
]

_RAW_TASKS_COLUMNS = [
    "id", "title", "description", "status", "due_date",
    "owner_id", "owner_email", "project_id", "project_name",
    "created_at", "updated_at",
]


def get_spark() -> SparkSession:
    """
    Local SparkSession, capped at 2 cores (ADR-017) — this process runs
    inside the same container as `airflow standalone`'s webserver,
    scheduler, and triggerer (ADR-001), so local[*] would compete with
    those for every core in the container instead of a bounded share.
    """
    return (
        SparkSession.builder
        .appName("flowhouse-transform")
        .master("local[2]")
        .getOrCreate()
    )


def _flatten_owner_project(df: DataFrame, project_sentinel: bool) -> DataFrame:
    """
    Flattens the nested owner/project struct columns (as they arrive
    from extract's tasks parquet) into flat owner_id/owner_email/
    project_id/project_name columns.

    project_sentinel=True  → missing project becomes (0, "(no project)")
                              — matches build_daily_task_snapshot's
                              grouping sentinel in pandas_ops.py.
    project_sentinel=False → missing project stays a literal null in
                              both fields — matches build_raw_tasks'
                              faithful-mirror semantics (ADR-012).

    owner is never missing (required FK, on_delete=CASCADE — see
    TaskTracker's apps/tasks/models.py), so no null handling needed there.
    """
    df = df.withColumn("owner_id", F.col("owner.id"))
    df = df.withColumn("owner_email", F.col("owner.email"))

    if project_sentinel:
        df = df.withColumn(
            "project_id",
            F.when(F.col("project").isNull(), F.lit(_NO_PROJECT_ID))
             .otherwise(F.col("project.id")),
        )
        df = df.withColumn(
            "project_name",
            F.when(F.col("project").isNull(), F.lit(_NO_PROJECT_NAME))
             .otherwise(F.col("project.name")),
        )
    else:
        df = df.withColumn("project_id", F.col("project.id"))
        df = df.withColumn("project_name", F.col("project.name"))

    return df


def build_daily_task_snapshot(spark: SparkSession, tasks_path: str, snapshot_date: str):
    """
    PySpark equivalent of pandas_ops.build_daily_task_snapshot().

    Same grouping (project_id, project_name, owner_id, owner_email,
    status), same overdue definition (due_date < snapshot_date AND
    status != "done" AND due_date is not null).

    Returns a pandas DataFrame (via .toPandas()) — see the module
    docstring and the "IMPORTANT — verify before relying on this"
    note below about snapshot_date's dtype after conversion.
    """
    tasks_df = spark.read.parquet(tasks_path)

    if tasks_df.rdd.isEmpty():
        empty_pdf = spark.createDataFrame([], schema=tasks_df.schema).toPandas()
        return empty_pdf.reindex(columns=_SNAPSHOT_OUTPUT_COLUMNS)

    flat = _flatten_owner_project(tasks_df, project_sentinel=True)

    flat = flat.withColumn(
        "is_overdue",
        (F.col("due_date") < F.lit(snapshot_date))
        & (F.col("status") != "done")
        & F.col("due_date").isNotNull(),
    )

    grouped = (
        flat.groupBy("project_id", "project_name", "owner_id", "owner_email", "status")
        .agg(
            F.count("id").alias("task_count"),
            F.sum(F.col("is_overdue").cast("int")).alias("overdue_count"),
        )
        .withColumn("snapshot_date", F.lit(snapshot_date).cast(T.DateType()))
        .select(_SNAPSHOT_OUTPUT_COLUMNS)
    )

    return grouped.toPandas()


def build_raw_tasks(spark: SparkSession, tasks_path: str):
    """
    PySpark equivalent of pandas_ops.build_raw_tasks() — one row per
    task, project stays a literal null when missing (no sentinel,
    mirrors ADR-012).

    Returns a pandas DataFrame (via .toPandas()). load_raw_tasks()
    already runs _normalize_nullable_columns() on its input regardless
    of which engine produced it, so the same nullable-column handling
    from ADR-012 applies here unchanged — no load-side changes needed.
    See the module docstring's caveat about due_date/created_at dtypes.
    """
    tasks_df = spark.read.parquet(tasks_path)

    if tasks_df.rdd.isEmpty():
        empty_pdf = spark.createDataFrame([], schema=tasks_df.schema).toPandas()
        return empty_pdf.reindex(columns=_RAW_TASKS_COLUMNS)

    flat = _flatten_owner_project(tasks_df, project_sentinel=False)
    flat = flat.withColumn("due_date", F.col("due_date").cast(T.DateType()))
    flat = flat.withColumn("created_at", F.col("created_at").cast(T.TimestampType()))
    flat = flat.withColumn("updated_at", F.col("updated_at").cast(T.TimestampType()))
    flat = flat.select(_RAW_TASKS_COLUMNS)

    return flat.toPandas()
