"""
Run inside the Airflow container, where Airflow itself is installed:

    docker compose exec airflow pytest /opt/airflow/tests

(Not runnable from a bare host venv unless apache-airflow is installed
there too — it's a fairly heavy dependency to add just for linting.)
"""
from airflow.models import DagBag


def test_dagbag_imports_without_errors():
    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    assert dagbag.import_errors == {}


def test_health_check_dag_loaded():
    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    dag = dagbag.get_dag("health_check")

    assert dag is not None
    assert set(dag.task_ids) == {"start", "check_tasktracker", "check_clickhouse", "end"}
