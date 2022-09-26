import boto3
import cx_Oracle
import csv
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

s3 = boto3.client("s3")

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
    """

    :param event:
    :param context:
    :return:
    """

    # Datetime execution
    bucket = event["bucket"]
    bucket_path = event["bucket_path"]
    schema = event["schema"]
    table_name = event["table_name"]
    upsert_col = event["upsert_col"]
    exec_dt = event["dt_execution"]
    interval = event["interval"]

    # Database credential data
    host = event["host"]
    port = event["port"]
    tns = event["tns"]
    user = event["user"]
    psswd = event["psswd"]

    # Getting the datetimes
    dt_strings = compute_datetime(airflow_ts=exec_dt, interval=interval)

    try:

        logger.info("Opening the connection")
        dsn_tns = cx_Oracle.makedsn(host, port, service_name=tns)
        print(f"TNS: {dsn_tns}")

        conn = cx_Oracle.connect(
            user=user,
            password=psswd,
            dsn=dsn_tns,
            encoding="UTF-8"
        )
        print(f"conn: {conn}")

        logger.info("Creating the cursor")
        oracle_cursor = conn.cursor()

        # Getting the name of the columns
        logger.info("Getting the table's columns")
        oracle_cursor.execute(f"SELECT column_name FROM USER_TAB_COLUMNS WHERE table_name = '{table_name}'")
        header_raw = oracle_cursor.fetchall()
        print(f"header: {header_raw}")

        header = [cn[0] for cn in header_raw]
        # print(f"Header: {header}")
        logger.debug(f"Header: {header}")

        # Creating the CSV file name
        logger.info("Creating the CSV file name")
        csv_path = f"{bucket_path}/{dt_strings['actual_ds_str']}.csv"
        logger.debug(f"CSV file name: {csv_path}")
        # print(f"CSV file name: {csv_path}")
        # print(f"dt_formated_to_oracle: {dt_formated_to_oracle}")

        logger.info("Get the data from the table")
        oracle_cursor.execute(
            f"SELECT * FROM {schema}.{table_name} WHERE {upsert_col} >= TO_TIMESTAMP('{dt_strings['prev_ds_str']}', 'yyyy-mm-dd hh24:mi:ss') AND {upsert_col} <= TO_TIMESTAMP('{dt_strings['actual_ds_str']}', 'yyyy-mm-dd hh24:mi:ss')"
        )

        raw_data = oracle_cursor.fetchall()
        # raw_data = oracle_cursor.fetchmany(10000)
        # print(f"raw_data: {raw_data}")

        # Checking if there are data to be extracted
        if len(raw_data) > 0:

            # Listing the values
            ls_data = [list(row) for row in raw_data]
            logger.debug(f"First row: {ls_data[0]}")
            logger.debug(f"Second row: {ls_data[1]}")

            with open(f"/tmp/{schema}-{table_name}-data.csv", "a", encoding="UTF8", newline="") as raw_file:

                # Creating the csv writer
                csv_writer = csv.writer(raw_file, delimiter=";")
                logger.debug("CSV writer created")

                # To writing the header
                csv_writer.writerow(header)
                logger.debug("CSV header written")

                # # To writing the data
                csv_writer.writerows(ls_data)
                logger.debug("CSV data written")
                # print(f"Raw data: {ls_data}")

                raw_file.close()
                logger.debug("CSV file closed")

            s3.upload_file(f"/tmp/{schema}-{table_name}-data.csv", bucket, csv_path)

        else:

            logger.info("There aren't data to be extracted")

        logger.info("Closing the DB connection")
        conn.close()

    except Exception as e:

        logger.debug(
            f"Table: {table_name} - Bucket: {bucket} - Execution datetime: {exec_dt}"
        )

        logger.error(e)

        return Exception(e)
