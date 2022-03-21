import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from airflow import DAG

from functions_utils.utils import (  # isort:skip
    alert_slack_channel,
    cleanup_xcom,
    get_file_names,
    read_local_file,
    send_slack_finish,
)

from airflow_dags.jobsity.utils import (  # isort:skip
    create_tables_group_task,
    create_chunk_data,
    load_chunks,
)

log = logging.getLogger(__name__)

files_path = "/" + "/".join(Path(__file__).parts[1:-1]) + "/files"

create_tables_queries_path = "files/create_tables"
create_table_names_stg = get_file_names(files_path, "stg")
create_table_names_ldw = get_file_names(files_path, "ldw")
load_chunks_names = get_file_names(f"{files_path}/consume", "chunks")
load_trips_ldw_data_path = f"{files_path}/queries/insert_trips_ldw.sql"
load_trips_by_area_ldw_data_path = f"{files_path}/queries/insert_trips_by_area_ldw.sql"

docs = """
Create source tables and load data into them from CSV
Project link
[@here](https://github.com/Oracy/airflow_dags/tree/2a5529a0671f91572888aa418d828d53324d6341/eu_efsa_food)
"""

default_args = {
    "owner": "Oracy Martos",
    "schedule_interval": "*/5 * * * *",
    "start_date": datetime(2022, 2, 11, 6, 00, 00),
    "catchup": False,
    "retries": 2,
    "on_failure_callback": alert_slack_channel,
    "dagrun_timeout": timedelta(minutes=60),
    "trigger_rule": "none_failed",
}

dag = DAG(
    "jobisty_ingest_data",
    default_args=default_args,
    tags=[
        "Jobsity",
        "Ingest_Data",
        "Test",
    ],
    max_active_runs=1,
    on_success_callback=cleanup_xcom,
    doc_md=docs,
)

with dag:

    start_flow_task = DummyOperator(task_id="start_flow")

    # files = ["trips", "big_trips"]
    files = ["trips"]
    consume_new_file_tasks = []
    for file in files:
        consume_new_file_task = FileSensor(
            task_id=f"Consume_{file}_sensor",
            poke_interval=30,
            filepath=f"{files_path}/consume/{file}.csv",
        )
        consume_new_file_tasks.append(consume_new_file_task)

    create_stg_table_task = DummyOperator(task_id="create_stg_table")

    create_table_tasks_group_stg = create_tables_group_task(
        create_table_names_stg,
        create_tables_queries_path,
        "stg",
        dag,
    )

    create_ldw_table_task = DummyOperator(task_id="create_ldw_table")

    create_table_tasks_group_ldw = create_tables_group_task(
        create_table_names_ldw,
        create_tables_queries_path,
        "ldw",
        dag,
    )

    create_chunk_data_task = PythonOperator(
        task_id="create_chunk_data",
        provide_context=True,
        python_callable=create_chunk_data,
        op_kwargs={
            "chunk_size": 10,
            "file_path": f"{files_path}/consume",
            # "file_name": 'big_trips',
            "file_name": "trips",
        },
        dag=dag,
    )

    load_chunks_task = PythonOperator(
        task_id="load_stg_data",
        provide_context=True,
        python_callable=load_chunks,
        op_kwargs={
            "relative_path": f"{files_path}/consume/chunks",
            "files_name": load_chunks_names,
            "table_insert": "trips_",
            "step": "stg",
        },
        dag=dag,
    )

    load_trips_ldw_data_query = read_local_file(load_trips_ldw_data_path)
    load_trips_ldw_data_task = PostgresOperator(
        task_id="load_trips_ldw_data",
        sql=load_trips_ldw_data_query,
        postgres_conn_id="postgres_data_source",
        database="postgres",
    )

    load_trips_by_area_ldw_data_query = read_local_file(load_trips_by_area_ldw_data_path)
    load_trips_by_area_ldw_data_task = PostgresOperator(
        task_id="load_trips_by_area_ldw_data",
        sql=load_trips_by_area_ldw_data_query,
        postgres_conn_id="postgres_data_source",
        database="postgres",
    )

    send_slack_message = PythonOperator(
        task_id="send_slack_message",
        provide_context=True,
        python_callable=send_slack_finish,
        dag=dag,
    )

    end_flow_task = DummyOperator(task_id="end_flow")

    start_flow_task >> consume_new_file_tasks
    consume_new_file_tasks >> create_stg_table_task
    create_stg_table_task >> create_table_tasks_group_stg
    create_table_tasks_group_stg >> create_ldw_table_task
    create_ldw_table_task >> create_table_tasks_group_ldw
    create_table_tasks_group_ldw >> create_chunk_data_task
    create_chunk_data_task >> load_chunks_task
    load_chunks_task >> load_trips_ldw_data_task
    load_trips_ldw_data_task >> load_trips_by_area_ldw_data_task
    load_trips_by_area_ldw_data_task >> send_slack_message
    send_slack_message >> end_flow_task

if __name__ == "__main__":
    dag.cli()
