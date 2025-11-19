import os
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import parse_dsn
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

def to_postgresql(dbname: str, table_name: str, df: pd.DataFrame, if_exists: str = "append") -> None:
    """
    Write a pandas DataFrame to a PostgreSQL database table.
    Creates the database if it doesn't exist.
    Creates the table if it doesn't exist.
    Appends data to the table if it already exists.

    Args:
        dbname (str): The name of the PostgreSQL database.
        table_name (str): The name of the table to write to.
        df (pd.DataFrame): The DataFrame to write to the database.
        if_exists (str, optional): What to do if the table already exists.
            Options: 'append', 'replace', 'fail'. Defaults to 'append'.
    """
    # Get DSN from environment variable
    dsn = os.getenv("DSN")
    if not dsn:
        raise ValueError("DSN environment variable is not set")

    # Parse DSN into connection parameters using psycopg2's built-in function
    conn_params = parse_dsn(dsn)

    # Update dbname with the provided parameter
    conn_params["dbname"] = dbname

    # Check if the database exists. If not, create it.
    try:
        # Try to connect to the database
        with psycopg2.connect(**conn_params):
            pass  # Connection successful
    except psycopg2.OperationalError as e:
        if "does not exist" in str(e):
            # Connect to default database (postgres) to create the new database
            pg_conn_params = conn_params.copy()
            pg_conn_params["dbname"] = "postgres"
            # Avoid using context manager for this connection to ensure autocommit is set correctly
            pg_conn = psycopg2.connect(**pg_conn_params)
            pg_conn.rollback()  # Roll back any existing transaction
            pg_conn.autocommit = True  # Set autocommit immediately after connecting
            try:
                cursor = pg_conn.cursor()
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
                print(f"Created database: {dbname}")
                cursor.close()
            finally:
                pg_conn.close()  # Manually close the connection
        else:
            # Re-raise other connection errors
            raise

    # Create SQLAlchemy engine URL
    db_url = URL.create(
        drivername="postgresql",
        username=conn_params["user"],
        password=conn_params["password"],
        host=conn_params["host"],
        port=conn_params["port"],
        database=conn_params["dbname"]
    )

    # Create SQLAlchemy engine
    engine = create_engine(db_url)

    # Write DataFrame to PostgreSQL table
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists=if_exists,
        index=False,
        schema="public"
    )

    print(f"Successfully wrote {len(df)} rows to {dbname}.{table_name}")