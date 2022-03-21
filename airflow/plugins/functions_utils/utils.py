import logging
import os
from typing import Any, Dict, List, Union

import pandas as pd
from airflow.models import XCom
from airflow.utils.db import provide_session
from sqlalchemy import create_engine

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator  # isort:skip


logger = logging.getLogger("Airflow Utils")


@provide_session
def cleanup_xcom(session=None, **kwargs: Dict[str, Any]) -> None:
    """Clean up xcom, to avoid keep trash and use database space incorrectly.

    Args:
        kwargs: Airflow kwargs, where TaskInstance (ti) will be used.
        session: Airflow session to be deleted receives None, because TaskInstance will replace
        with the correct session.

    Returns:
        There is no Return.
    """
    ti = kwargs["task_instance"]
    session.query(XCom).filter(
        XCom.dag_id == ti.dag_id, XCom.execution_date == ti.execution_date
    ).delete()


def alert_slack_channel(kwargs: Dict[str, Any]) -> None:
    """Send message to slack if task fail, using function on_failure_callback.

    Args:
        kwargs: It has all variables that will be used to create default message.
        kwargs["task_instance"]: Class TaskInstance with all values that we need
        from airflow task operator.
        kwargs["task_instance"]["task_id"]: Airflow Task name that failed.
        kwargs["task_instance"]["dag_id"]: Airflow Task name that failed.
        kwargs["execution_date"]: Execution date that task runs.
        kwargs["exception"]: What was the exception that Task suffered.
        kwargs["reason"]: What was the reason that Task suffered.
        kwargs["task_instance"]["log_url"]: Log url to Airflow local.
        kwargs["task_instance"]["duration"]: How many time that Task run before failed.

    Returns:
        There is no return
    """
    last_task = kwargs.get("task_instance")
    message_variables: Dict[str, Any] = {
        "dag_id": last_task.dag_id,
        "task_id": last_task.task_id,
        "execution_date": kwargs.get("execution_date"),
        "error_message": kwargs.get("exception", "reason"),
        "log_url": last_task.log_url,
        "duration": last_task.duration,
    }
    title: str = (
        f':red_circle: AIRFLOW DAG *{message_variables["dag_id"]}*'
        f' - TASK *{message_variables["task_id"]}* has failed! :boom:'
    )
    msg_parts: Dict[str, str] = {
        "Execution date": message_variables["execution_date"],
        "Error": message_variables["error_message"],
        "Log url": message_variables["log_url"],
        "Task Duration": message_variables["duration"],
    }
    msg: str = "\n".join([title, *[f"*{k}*: {v}" for k, v in msg_parts.items()]]).strip()
    SlackWebhookOperator(
        task_id="notify_slack_channel_alert", http_conn_id="slack_webhook", message=msg
    ).execute(context=None)


def read_parquet(file_name, **kwargs: Dict[str, Any]) -> pd.DataFrame:
    """Read data from parquet to call function to insert data into postgres database.

    Args:
        **relative_path: File path that parquet are stored, to read it on local folders.

    Returns:
        Only a f-string with which table were inserted. For example:
        acute_g_day_bw_all_days Done!
    """
    relative_path: str = kwargs.get("relative_path")
    home_path: str = os.environ["HOME"]
    file_path: str = os.path.join(home_path, "dags", f"{relative_path}/{file_name}")
    df: pd.DataFrame = pd.read_parquet(file_path)
    return df


def get_file_names(files_path: str, folder_name: str) -> List[Union[List[str], List[str]]]:
    """Get all file names that would be loaded, from specific folder and specific path.

    Args:
        files_path: File path that parquet are stored, to read it on local folders.
        folder_name: Folder name to find files.

    Returns:
        An array with all create tables. For example:
        ["acute_g_day_bw_all_days", "acute_g_day_bw_cons_days", "chronic_g_day_bw_to_t_pop", ...]
    """
    files_name = None
    for root, directories, files in os.walk(files_path):
        if folder_name in root:
            files_name = files
    files_name_regexp = [name.replace(".sql", "") for name in files_name]
    return files_name_regexp


def write_parquet_to_postgres(file_name: str, **kwargs: Dict[str, Any]) -> None:
    """Insert data from parquet to postgres database using sqlalchemy create_engine.

    Args:
        **file: Which database that dataframe would be inserted.

    Returns:
        Only a f-string with which table were inserted. For example:
        trips Writed!
    """
    table_insert = (
        kwargs.get("table_insert") if "week_of_year_by_area" not in file_name else "trips_by_area_"
    )
    step = kwargs.get("step")
    df: pd.DataFrame = read_parquet(file_name, **kwargs)
    engine = create_engine("postgresql://postgres:postgres@172.19.0.2:5432/postgres")
    df.to_sql(f"{table_insert}{step}", engine, if_exists="append", index=False)


def read_local_file(file_relative_path: str) -> Any:
    """Load local file to memory.

    Args:
        file_relative_path: physical path where the SQL file is stored.

    Returns:
        Return data from file, it can be, sql, txt, markdown, any type.
    """
    home_path = os.environ["HOME"]
    file_path = os.path.join(home_path, "dags", file_relative_path)
    with open(file_path, "r") as file:
        file = file.read()
    return file


def send_slack_finish(**kwargs) -> None:
    """Send message to slack if task fail, using function on_failure_callback.

    Args:
        kwargs: It has all variables that will be used to create default message.
        kwargs["task_instance"]: Class TaskInstance with all values that we need
        from airflow task operator.
        kwargs["task_instance"]["task_id"]: Airflow Task name that failed.
        kwargs["task_instance"]["dag_id"]: Airflow Task name that failed.
        kwargs["execution_date"]: Execution date that task runs.
        kwargs["exception"]: What was the exception that Task suffered.
        kwargs["reason"]: What was the reason that Task suffered.
        kwargs["task_instance"]["log_url"]: Log url to Airflow local.
        kwargs["task_instance"]["duration"]: How many time that Task run before failed.

    Returns:
        There is no return
    """
    last_task = kwargs.get("task_instance")
    message_variables: Dict[str, Any] = {
        "dag_id": last_task.dag_id,
        "task_id": last_task.task_id,
        "execution_date": kwargs.get("execution_date"),
        "log_url": last_task.log_url,
        "duration": last_task.duration,
    }
    title: str = (
        f':green-heavy-check-mark: AIRFLOW DAG *{message_variables["dag_id"]}*'
        f' - TASK *{message_variables["task_id"]}* have been finished!'
    )
    msg_parts: Dict[str, str] = {
        "Execution date": message_variables["execution_date"],
        "Log url": message_variables["log_url"],
        "Task Duration": message_variables["duration"],
    }
    msg: str = "\n".join([title, *[f"*{k}*: {v}" for k, v in msg_parts.items()]]).strip()
    SlackWebhookOperator(
        task_id="notify_slack_channel", http_conn_id="slack_webhook", message=msg
    ).execute(context=None)
