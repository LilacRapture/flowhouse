"""
DAG-import and structure test for health_check.

DAGS_FOLDER is computed relative to this file (../dags), not hardcoded
to /opt/airflow/dags.
"""
import os

from airflow.models import DagBag

DAGS_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dags")


def test_dagbag_imports_without_errors():
    dagbag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    assert dagbag.import_errors == {}


def test_health_check_dag_loaded():
    dagbag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    dag = dagbag.get_dag("health_check")

    assert dag is not None
    assert set(dag.task_ids) == {"start", "check_tasktracker", "check_clickhouse", "end"}
