import awswrangler as wr
import psycopg2
from datetime import datetime, timedelta

def compute_datetime(airflow_ts=str, interval=str) -> dict:
    """

    :param airflow_ts:
    :return:
    """

    # Transforming the string exec_dt into a datatetime obj
    runtime_dt_obj = datetime.fromisoformat(airflow_ts) - timedelta(hours=3)

    # Formatting the datetime obj
    actual_ds_str = runtime_dt_obj.strftime("%Y-%m-%d %H:%M:%S")

    if interval == "@hourly":

        # Datetime object with 1 hour less
        prev_exec_dt_obj = runtime_dt_obj - timedelta(hours=1)

    elif interval == "@daily":

        # Datetime object with 3 less
        prev_exec_dt_obj = runtime_dt_obj - timedelta(hours=24)

    elif interval == "@weekly":

        # Datetime object with 3 less
        prev_exec_dt_obj = runtime_dt_obj - timedelta(days=7)

    else:
        raise Exception

    # Convert the Airflow execution datetime to a specific format
    prev_ds_str = prev_exec_dt_obj.strftime("%Y-%m-%d %H:%M:%S")

    return {"actual_ds_str": actual_ds_str, "prev_ds_str": prev_ds_str}

def generate_query(constraint_conflict: str, table: str, columns: list) -> str:
    """

    :param constraint_conflict:
    :param table:
    :param columns:
    :return:
    """

    cols = [columns[i] for i in range(0, len(columns))]

    # Defining the start of the SQL query and adding the name of the variables to be updated.
    insert = "INSERT INTO " + table + " (" + ", ".join(cols)

    # Defining the number of columns to be updated.
    nCols = len(cols)

    # Dynamically creating the section of the query that defines the values.
    # Dynamically inserting the '%s% and defining which is the conflicting variable.
    values = ") VALUES (" + "".join(["%s, " if i != nCols - 1 else "%s" for i in
                                     range(0, nCols)]) + ") ON CONFLICT " + constraint_conflict + " DO UPDATE SET"

    # Dynamically creating the query section that defines the update commands.
    # u = [c + " = EXCLUDED." + c for c in cols]
    excluded = [c + "=EXCLUDED." + c for c in cols]

    # Joining the list into a single string separated by commas.
    updateSet = ",".join(excluded) + ";"

    # Insert additional columns in the query.
    # if "updated_at" in othersColumnsForUpdate:

    # updateSet += ", updated_at = NOW()"

    # Defining the variables to be returned by the query.
    # returning = "RETURNING " + ",".join(returnVariables) + ";"

    # Gathering all the parts of the query in a list.
    query = [insert, values, updateSet]

    # Joining all parts of the query and returning the result obtained.
    final_query = " ".join(query)

    return final_query

def lambda_handler(event, context):
    """

    :param event:
    :param context:
    :return:
    """

    # Datetime execution
    bucket = event["source_bucket"]
    table_name = event["table_name"]
    exec_dt = event["dt_execution"]
    interval = event["interval"]
    constraint_conflict = event["constraint_conflict"]

    # Database credential data
    host = event["host"]
    port = event["port"]
    db = event["db"]
    user = event["user"]
    psswd = event["psswd"]

    try:

        dt_strings = compute_datetime(airflow_ts=exec_dt, interval=interval)

        # Reading the parquet file
        parquet_df = wr.s3.read_parquet(
            path=[f"s3://{bucket}/{db}/{table_name}/{dt_strings['actual_ds_str']}.parquet"],
            dataset=False,
            use_threads=True
        )

        pqt_dct = parquet_df.to_dict(orient="split")

        # Cleaning the memory
        del parquet_df

        generated_query = generate_query(
            constraint_conflict=constraint_conflict,
            table=table_name,
            columns=pqt_dct["columns"]
        )

        conn = psycopg2.connect(
            dbname=db,
            host=host,
            port=port,
            user=user,
            password=psswd
            )

        cursor = conn.cursor()

        for row in pqt_dct["data"]:

            cursor.execute(query=generated_query, vars=tuple(row))

        conn.commit()

        conn.close()

        return True

    except Exception as e:
        raise Exception(e)
