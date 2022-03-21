import shutil
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from functions_utils.utils import write_parquet_to_postgres

from airflow import DAG


def drop_duplicated(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicated rows, group by ["origin_coord", "destination_coord", "datetime"]

    Args:
        df: A DataFrame with source data.

    Returns:
        A DataFrame without duplicated values.
    """
    # Trips with similar origin, destination, and time of day should be grouped together.
    df = df.drop_duplicates(subset=["origin_coord", "destination_coord", "datetime"])
    return df


def convert_datetime_type(df: pd.DataFrame) -> pd.DataFrame:
    """Convert datetime column from object to datetime type

    Args:
        df: A DataFrame with source data.

    Returns:
        A DataFrame with datetime column as datetime type.
    """
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df


def remove_characters(df: pd.DataFrame) -> pd.DataFrame:
    """Remove excedent characteres from origin_coord and destination_coord columns

    Args:
        df: A DataFrame with source data.

    Returns:
        A DataFrame without POINT (*) on columns origin_coord and destination_coord.
    """
    df["origin_coord"] = df["origin_coord"].str.replace("POINT \(", "", regex=True)
    df["origin_coord"] = df["origin_coord"].str.replace("\)", "", regex=True)
    df["destination_coord"] = df["destination_coord"].str.replace("POINT \(", "", regex=True)
    df["destination_coord"] = df["destination_coord"].str.replace("\)", "", regex=True)
    return df


def create_long_lat(df: pd.DataFrame) -> pd.DataFrame:
    """Split columns origin_coord and destination_coord into lat_origin, long_origin,
    lat_destination, long_destination

    Args:
        df: A DataFrame with source data.

    Returns:
        A DataFrame with 4 new columns lat_origin, long_origin, lat_destination, long_destination.
    """
    df[["lat_origin", "long_origin"]] = df["origin_coord"].str.split(" ", 1, expand=True)
    df[["lat_destination", "long_destination"]] = df["destination_coord"].str.split(
        " ", 1, expand=True
    )
    return df


def remove_excedent_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove columns origin_coord and destination_coord that will not be used

    Args:
        df: A DataFrame with source data.

    Returns:
        A DataFrame without columns origin_coord and destination_coord.
    """
    df = df.drop(labels=["origin_coord", "destination_coord"], axis=1)
    return df


def create_week_of_year_by_area(df: pd.DataFrame) -> pd.DataFrame:
    """Create new DataFrame with columns region, weekofyear and count.
    Which count is the number of times that trips happen at that group

    Args:
        df: A DataFrame with source data.

    Returns:
        A DataFrame with columns region, weekofyear and count.
    """
    # Develop a way to obtain the weekly average number of trips for an area, defined by a bounding box (given by coordinates) or by a region.
    df["weekofyear"] = df["datetime"].dt.isocalendar().week
    trips_by_area = df.groupby(["region", "weekofyear"]).size().reset_index(name="count")
    return trips_by_area


def create_tables_group_task(
    create_table_names: List[str], create_tables_queries_path: str, step: str, dag: DAG
) -> TaskGroup:
    """Create group task on flow with sql queries that create tables on database.

    Args:
        create_table_names: An array with table name that whould be created.
        create_tables_queries_path: Path where create queries are stored.
        dag: dag parameter to set operator.

    Returns:
        A TaskGroup Operator with an array with all create tables. For example:
        ["acute_g_day_bw_all_days", "acute_g_day_bw_cons_days", "chronic_g_day_bw_to_t_pop", ...]
    """
    with TaskGroup(group_id=f"create_table_group_{step}") as create_table_tasks_group:
        create_table_tasks: List[PostgresOperator] = []
        for create_table in create_table_names:
            last_insert_task: PostgresOperator = PostgresOperator(
                task_id="create_table_" + create_table + "_" + step,
                postgres_conn_id="postgres_data_source",
                sql=f"{create_tables_queries_path}/{step}/{create_table}.sql",
                autocommit=True,
                dag=dag,
            )
            create_table_tasks.append(last_insert_task)
    return create_table_tasks_group


def create_chunk_data(**kwargs: Dict[str, Any]) -> pd.DataFrame:
    """Split large csv file into chunks, pre processing with functions above.

    Args:
        **task_instance: task instance to push xcom.
        **file_path: file path to load data.
        **file_name: file name to load data.
        **chunk_size: chunks size, can pass by parameter, or receive default value

    Returns:
        New df with limited size
    """
    ti = kwargs.get("task_instance")
    file_path = kwargs.get("file_path")
    file_name = kwargs.get("file_name")
    now = datetime.now()
    date_time = now.strftime("%m_%d_%Y")
    chunk_size = kwargs.get("chunk_size", 10000)
    part = 0
    df = pd.read_csv(f"{file_path}/{file_name}.csv", chunksize=chunk_size)
    for chunk in df:
        df_drop_duplicated = drop_duplicated(chunk)
        df_convert_datetime = convert_datetime_type(df_drop_duplicated)
        df_remove_characters = remove_characters(df_convert_datetime)
        df_create_long_lat = create_long_lat(df_remove_characters)
        df_remove_excedent_columns = remove_excedent_columns(df_create_long_lat)
        df_remove_excedent_columns.to_parquet(
            f"{file_path}/chunks/{file_name}_{date_time}_{part}.parquet",
            engine="pyarrow",
            index=False,
        )
        df_week_of_year_by_area = create_week_of_year_by_area(df_remove_excedent_columns)
        df_week_of_year_by_area.to_parquet(
            f"{file_path}/chunks/{file_name}_df_week_of_year_by_area_{date_time}_{part}.parquet",
            engine="pyarrow",
            index=False,
        )
        part += 1
    ti.xcom_push(key="chunks_count", value=part - 1)
    move_files(file_path, file_name, date_time)


def move_files(file_path: str, file_name: str, information: str, chunk=0) -> None:
    """Move file from consule to done foldes.

    Args:
        file_path: file path to save data.
        file_name: file name to save data.
        information: information about that processing, if it will receive a datetime or "processed" value.
        chunk: Boolean, to check if it is processing chunk or DF.

    Returns:
        None
    """
    if chunk:
        src_path = f"{file_path}/{file_name}"
        dst_path = f"{file_path}/../../done/chunks/{information}_{file_name}"
    else:
        src_path = f"{file_path}/{file_name}.csv"
        dst_path = f"{file_path}/../done/{file_name}_{information}.csv"
    shutil.move(src_path, dst_path)


def load_chunks(**kwargs: Dict[str, Any]):
    """Load chunks to Postgres.

    Args:
        **relative_path: file path to save data.
        **file_name: file name to save data.

    Returns:
        None
    """
    files_name = kwargs.get("files_name")
    file_path = kwargs.get("relative_path")
    for file_name in files_name:
        write_parquet_to_postgres(file_name, **kwargs)
        move_files(file_path, file_name, "processed", 1)
