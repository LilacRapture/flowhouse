"""
DAG-import and structure test for sync_tasktracker_to_clickhouse — run
inside the Airflow container, same as test_health_check_dag.py:

    docker compose exec airflow pytest /opt/airflow/tests

extract_tasks_task/load_snapshot_task/load_raw_task themselves need a
live Airflow Connection (tasktracker_api) and a reachable ClickHouse.
"""
from airflow.models import DagBag


def test_dagbag_imports_without_errors():
    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    assert dagbag.import_errors == {}


def test_sync_dag_loaded_with_expected_tasks():
    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    dag = dagbag.get_dag("sync_tasktracker_to_clickhouse")

    assert dag is not None
    assert set(dag.task_ids) == {
        "extract_tasks_task",
        "transform_snapshot_task",
        "load_snapshot_task",
        "transform_raw_task",
        "load_raw_task",
    }


def test_extract_fans_out_to_both_transform_tasks():
    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    dag = dagbag.get_dag("sync_tasktracker_to_clickhouse")

    extract = dag.get_task("extract_tasks_task")
    downstream_ids = {t.task_id for t in extract.downstream_list}
    assert downstream_ids == {"transform_snapshot_task", "transform_raw_task"}


def test_transform_snapshot_leads_only_to_load_snapshot():
    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    dag = dagbag.get_dag("sync_tasktracker_to_clickhouse")

    transform_snapshot = dag.get_task("transform_snapshot_task")
    assert [t.task_id for t in transform_snapshot.downstream_list] == ["load_snapshot_task"]


def test_transform_raw_leads_only_to_load_raw():
    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    dag = dagbag.get_dag("sync_tasktracker_to_clickhouse")

    transform_raw = dag.get_task("transform_raw_task")
    assert [t.task_id for t in transform_raw.downstream_list] == ["load_raw_task"]
