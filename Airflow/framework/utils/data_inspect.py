from airflow.providers.oracle.hooks.oracle import OracleHook
from datetime import datetime, timedelta

class DataInspect:
    # def __int__(self):

    @staticmethod
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

    @staticmethod
    def oracle_check_table_data(ts=str, oracle_schema_metadata=list, schema=str, interval=str) -> list:
        """

        :param oracle_tns_metadata:

        :return:
        """

        # List of tables with data to be processed
        tables_with_data = []

        # Getting the datetimes
        dt_strings = DataInspect.compute_datetime(airflow_ts=ts, interval=interval)

        # Opening the Oracle DB connection
        conn = OracleHook(conn_name_attr="oracle_default").get_conn()

        for table in oracle_schema_metadata:

            # The table name
            table_name = list(table.keys())[0]

            # Setting the task_id
            tk_id = f"{schema}.{table_name}_extrac"

            # Getting the table datetime column
            upsert_col = table[table_name]["upsert_col"]

            # Query to be executed into DB
            query = f"SELECT count(*) AS count FROM {table_name} \
             WHERE {upsert_col} >= TO_TIMESTAMP('{dt_strings['prev_ds_str']}', 'yyyy-mm-dd hh24:mi:ss') \
              AND {upsert_col} <= TO_TIMESTAMP('{dt_strings['actual_ds_str']}', 'yyyy-mm-dd hh24:mi:ss')"

            # Setting the DB cursor
            cursor = conn.cursor()

            # Executing the query
            cursor.execute(query)

            # Getting the number of records
            records = cursor.fetchone()
            print(f"records: {records}")

            if records[0] > 0:
                tables_with_data.append(tk_id)

            cursor.close()

        conn.close()

        return tables_with_data
