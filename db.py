import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text
from core import config


def db_connection():
    endpoint = config["DB_ENDPOINT"]
    db_user = config["DB_USER"]
    db_password = config["DB_PASSWORD"]
    connection_string = f"postgresql://{db_user}:{db_password}@{endpoint}/postgres"
    return create_engine(connection_string)


def read_data(cols):
    connection = db_connection()
    return pd.read_sql_query(f'select {", ".join(cols)} from jira_issues', connection)


def write_data(
    data: dict,
    table_name,
    conflicts,
    cols,
):
    connection = db_connection()
    query = text(
        f"""
            insert into {table_name} ({", ".join(cols)}) values
            {'(:'+', :'.join(cols)+')'}
            on conflict ({', '.join(conflicts)}) do update
            set {', '.join([f"{c} = excluded.{c}" for c in set(cols)-set(conflicts)])};
        """
    )
    connection.execute(query, data)


def dataframe_to_db(data: pd.DataFrame, table_name, conflicts, cols) -> None:
    rows = df_to_rows(data)
    for row in rows:
        write_data(row, table_name=table_name, conflicts=conflicts, cols=cols)


def df_to_rows(data: pd.DataFrame) -> list:
    return [row.to_dict() for _, row in data.iterrows()]
