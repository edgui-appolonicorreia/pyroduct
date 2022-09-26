import awswrangler as wr
import logging
from datetime import datetime, timedelta

# Create and configure logger
logging.basicConfig(
    # filename="newfile.log",
    format="%(name)s -> %(levelname)s: %(message)s",
    filemode="a",
    level=logging.DEBUG
)

# Creating an object
logger = logging.getLogger()

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


def lambda_handler(event, context):

    # S3 Infos
    dt_execution = event["dt_execution"]
    source_bucket = event["source_bucket"]
    target_bucket = event["target_bucket"]
    schema = event["schema"]
    table_name = event["table_name"]
    interval = event["interval"]

    formatted_exec_obj = compute_datetime(airflow_ts=dt_execution, interval=interval)

    # Getting the CSV file name
    csv_name = f"{formatted_exec_obj['actual_ds_str']}.csv"
    print(f"csv_name: {csv_name}")

    try:

        csv_df = wr.s3.read_csv(path=f"s3://{source_bucket}/{schema}/{table_name}/{csv_name}", sep=";")
        print(csv_df)

        # Creating the file name
        #pqt_path_name = f"{formatted_exec_obj}.parquet"
        #print(f"parquet name: {pqt_path_name}")
        # logger.debug(f"File name: {pqt_path_name}")

        # Converting the csv into parquet file
        parquet_wrote = wr.s3.to_parquet(
            df=csv_df,
            path=f"s3://{target_bucket}/{schema}/{table_name}/{formatted_exec_obj['actual_ds_str']}.parquet",
            index=False,
            dataset=False,
            compression="snappy"
        )
        print(f"parquet_wrote: {parquet_wrote}")
        print("Step 3: upload done")

    except Exception as e:
        logger.debug(
            f"Database: {schema} - Table: {table_name} - Bucket: {source_bucket} - Execution datetime: {dt_execution}"
        )
        logger.error(e)
        raise Exception(e)
