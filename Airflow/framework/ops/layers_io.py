import json
import logging
import pandas as pd
from datetime import timedelta
from datetime import datetime as dt
from Airflow.framework.utils.Parquet import Parquet
from Airflow.framework.utils.AuxFunctions import DataAmount
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks import s3

class Bronze(Parquet, DataAmount):

    logging.basicConfig(format="%(process)d-%(levelname)s-%(message)s", level=logging.INFO)

    def __init__(self, bucket_conn: s3, bucket_name: str, amount_data: str):

        super().__init__()

        # Bucket connection
        self.bucket_conn = bucket_conn

        # Bucket name
        self.bucket_name = bucket_name

        # Amount of data -- ONLY 3 OPTIONS AVAILABLE -- [few, medium, huge]
        self.amount_of_data = DataAmount._amount_of_data(amount_of_data=amount_data)

    def get_data(self, dir: str, data_type: str) -> list:
        """"""

        try:

            # Getting all file names in the bucket
            file_list = self.bucket_conn.list_keys(bucket_name=self.bucket_name, prefix=dir)

            # Creating a list to load the file names from the specified directory.
            valid_records = []

            if data_type in ["txt", "csv"]:

                # Getting the files from the dir.
                for file_name in file_list:

                    # Verifying that the file name is not empty.
                    if file_name != "" or file_name != " " or file_name != (dir + "/"):

                        # Parsing the JSON, if not possible put it on a specific list and write as Invalid Record
                        try:

                            # Using the Airflow get_key method to get the object
                            obj = self.bucket_conn.get_key(key=file_name, bucket_name=self.bucket_name)

                            # The raw data is stored into the 'Body' key, and the next line, access it
                            file_to_read = obj.get()["Body"].iter_lines()

                            # Iterating through each file
                            for line in file_to_read:

                                # Appending the line into the valid_records list
                                valid_records.append(json.loads(line))

                        except Exception as error:

                            logging.error(error)

                return valid_records

            elif data_type == "parquet":

                # Getting the files from the dir.
                for file_name in file_list:

                    # Verifying that the file name is not empty.
                    if file_name != "" or file_name != " " or file_name != (dir + "/"):

                        # Parsing the JSON, if not possible put it on a specific list and write as Invalid Record
                        try:

                            # Using the Airflow get_key method to get the object
                            # obj = self.bucket_conn.get_key(key=file_name, bucket_name=self.bucket_name)

                            pqt_df = Parquet.parquet_to_df(
                                bucket_conn=self.bucket_conn,
                                bucket_name=self.bucket_name,
                                dir=dir,
                                parquet_name=file_name
                            )

                        except Exception as error:

                            logging.error(error)

            else:

                logging.critical("")

                raise RuntimeError("")

        except Exception as error:

            logging.critical(error)

            raise RuntimeError(error)

    def write_data(self, dataset: list, dir: str) -> bool:
        """
        Description:
            To write data at the Raw Zone, in a specific directory.

        Args:
            dataset: A list of objects of any type.
            dir: Path to directory where the data will write.

        Returns:
            A True value or raise an error.
        """

        try:

            for obj in dataset:

                # Saves object file in S3 bucket
                self.bucket_conn.load_file_obj(
                    file_obj=obj,
                    key=dir,
                    bucket_name=self.bucket_name
                )

            return True

        except Exception as error:

            logging.critical(error)

            raise RuntimeError(error)

class Silver:
    """
        Description:
            Class with methods to be used in the Refined zone.
    """

    logging.basicConfig(format="%(process)d-%(levelname)s-%(message)s", level=logging.INFO)

    def __init__(self, bucket_conn: s3, aurora_conn: PostgresHook):

        self.bucket_conn = bucket_conn

        self.aurora_conn = aurora_conn.get_conn()

    def get_np_trusted_data(self, trusted_bucket_name: str, trusted_dir: str) -> pd.DataFrame:
        """
        Description:
            Get the raw data from trusted zone.

        Args:
            trusted_bucket_name: Trusted bucket name.
            trusted_dir: Directory name in the trusted bucket with the not processed data.

        Returns:
            Dataframe with not processed data.
        """

        try:

            # Getting the list of files
            file_list = self.bucket_conn.list_keys(bucket_name=trusted_bucket_name, prefix=trusted_dir)

            # Cleaning the file list, dropping the directories strings
            file_list = [file for file in file_list if not file.endswith("/")]

            # Dataframe to append.
            not_processed_data = pd.DataFrame()

            # Converts parquets into dataframe.
            for file in file_list:

                # Getting the file name.
                file = file.split("/")[-1]

                # Converts parquet into dataframe.
                data = Parquet().parquet_to_df(
                    bucket_conn=self.bucket_conn,
                    bucket_name=trusted_bucket_name,
                    dir=trusted_dir,
                    parquet_name=file
                )

                # Append data into dataframe.
                not_processed_data = not_processed_data.append(data, ignore_index=True)

            return not_processed_data

        except Exception as error:

            logging.critical(error)

            raise ValueError("Error:", error)

    def send_data(self, query: str, values: list, return_value: bool):
        """
        Description:
            Send data to the Refined Zone, Aurora database.

        Args:
            query: a string with the complete query to be executed.
            values: a list of values will be inserted.
            return_value: if there are values, or one value to be returned, set as True.

        Returns:
            return some value if the parameter 'return_value' it's true, else just True value.
        """

        try:

            # Instancing the cursor
            cursor = self.aurora_conn.cursor()

            if return_value:

                # Executing the query
                cursor.execute(query, tuple(values))

                # Commiting the data inserted
                self.aurora_conn.commit()

                # Closing the cursor
                cursor.close()

                return cursor.fetchall()

            else:

                # Commiting the data inserted
                self.aurora_conn.commit()

                # Closing the cursor
                cursor.close()

                # Executing the query
                cursor.execute(query, tuple(values))

                return True

        except Exception as error:

            logging.critical(error)

            raise ValueError("Error at insert data into Refined Zone:", error)

    def read_data(self, schema: str, table: str, cursor, columns=None) -> pd.DataFrame:
        """
        Description:
            Read data from the Refined Zone.

        Args:
            schema: the name of the schema.
            table: the name of the table where it contains the data.
            columns: It's optional, a list of columns to be selected in the table.
            cursor: an object cursor type, from the database target where will be read the data.

        Returns:
            A dataframe type, containing the data.
        """

        try:

            # Checking if the 'column' parameter it's empty or not
            if columns is not None:

                # Number of columns
                number_cols = len(columns)

                # Creating the columns string
                formatted_columns = "".join(["%s, " if i != number_cols - 1 else "%s" for i in range(0, number_cols)])

                # Creating the SQL query
                query = f"SELECT {formatted_columns} FROM {schema}.{table};"

                # Executing the query
                cursor().execute(query)

                # Transforming the data into a dataframe
                df_ready = pd.DataFrame(cursor().fetchall())

                return df_ready

            else:

                # Creating the SQL query
                query = f"SELECT * FROM {schema}.{table};"

                # Executing the query
                cursor().execute(query)

                # Transforming the data into a dataframe
                df_ready = pd.DataFrame(cursor().fetchall())

                return df_ready

        except Exception as error:

            logging.critical(error)

            raise ValueError("Error:", error)

class Golden(Parquet):
    """
        Description:
            Class with methods to be used in the Golden layer.
    """

    logging.basicConfig(format="%(process)d-%(levelname)s-%(message)s", level=logging.INFO)

    def __init__(self, bucket_conn: s3, bucket_name: str):

        super().__init__()

        # S3 connection to bucket
        self.bucket_conn = bucket_conn

        # Name of the bucket
        self.bucket_name = bucket_name

    def write_data(self, dir: str, parquet_name: str, data: dict, exclude_columns: list = None) -> bool:
        """
        Description:
            Writes records in the parquet file from Bucket S3.

        Args:
            dir: Name of the directory in the S3 bucket that the file is located..
            parquet_name: Name of the parquet file.
            data: Data in dict format.
            exclude_columns: Columns to be excluded from dataframe.

        Returns:
            A True value or raise an error.
        """

        # Replace mutable default argument.
        if exclude_columns is None:

            exclude_columns = list()

        try:

            # Checking if parquet file exists.
            checker = self.check_parquet_file(
                bucket_conn=self.bucket_conn,
                bucket_name=self.bucket_name,
                dir=dir,
                parquet_name=parquet_name
            )

            # Removing columns from data.
            data = pd.DataFrame(data).drop(columns=exclude_columns)

            # Checking if parquet file exists.
            if checker:

                # Recording new records on the file.
                self.write_on_file(
                    bucket_conn=self.bucket_conn,
                    bucket_name=self.bucket_name,
                    dir=dir,
                    data=data.to_dict(),
                    parquet_name=parquet_name
                )

                # Log message.
                logging.info(f"New records were saved in file '{parquet_name}'.")

                return True

            else:

                # Creating the file and saving the records.
                self.df_to_parquet_to_s3(
                    bucket_conn=self.bucket_conn,
                    bucket_name=self.bucket_name,
                    dir=dir,
                    data=data.to_dict(),
                    parquet_name=parquet_name
                )

                # Log message.
                logging.info(f"File '{parquet_name}' was created and records were saved.")

                return True

        except Exception as error:

            # Log message.
            logging.critical(error)

    def read_parquet(self, bucket_name: str, dir: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Description:
            Reads the parquet according to a range of dates.

        Args:
            bucket_name: Name of the Bucket in S3.
            dir: Name of the directory in S3 bucket.
            start_date: Filter start date.
            end_date: Filter end date.

        Returns:
            Filtered dataframe.
        """

        try:

            # Adjusting the end date.
            end_date = str(dt.strptime(end_date, "%Y-%m-%d") + timedelta(1))[:10]

            # Dates of the parquets.
            dates = pd.date_range(start=start_date, end=end_date)

            # Parquet name.
            parquets = set(str(date)[:7] for date in dates)

            # Dataframe to store the data.
            data = pd.DataFrame()

            # Getting the data from bucket.
            for parquet in parquets:

                parquet_data = self.parquet_to_df(
                    bucket_conn=self.bucket_conn,
                    bucket_name=bucket_name,
                    dir=dir,
                    parquet_name=parquet
                )

                # Appending the data into the parquet file
                data = data.append(parquet_data, ignore_index=True)

            # Filtering the data.
            data = data[(data.trusted >= start_date) & (data.trusted <= end_date)]

            # Log message.
            logging.info("Data were filtered according to the dates entered.")

            return data

        except Exception as error:

            # Log message.
            logging.critical(error)

    def read_raw_data(self, bronze_bucket: str, not_processed_dir: str, processed_dir: str) -> list:
        """
        Description:
            Reads data from the bronze layer.

        Args:
            bronze_bucket: Raw bucket name.
            not_processed_dir: Directory name with unprocessed files.
            processed_dir: Directory name with processed files.

        Returns:
            List of dicts with records to be processed.
        """

        try:

            # Log message.
            logging.info("Bucket and directory checked! Going on with the DAG ...")

            # Getting all file names in the bucket.
            file_list = self.bucket_conn.list_keys(bucket_name=bronze_bucket, prefix=not_processed_dir)

            # Removing directory from list.
            file_list = [file for file in file_list if not file.endswith("/")]

            # Logging the number of files
            logging.info(f"Number of files read: {len(file_list)}.")

            # List to write records.
            valid_dicts = []

            for file in file_list:

                # Getting the object and reading it.
                obj = self.bucket_conn.get_key(key=file, bucket_name=bronze_bucket)

                # Iterating the records into rows.
                file_to_read = obj.get()["Body"].iter_lines()

                # Iterating through each file.
                for line in file_to_read:

                    # Appending the line into the valid_records list.
                    valid_dicts.append(json.loads(line))

                # Getting the source URL file.
                source_obj_url = "s3://" + bronze_bucket + "/" + file

                # Removing the name of the directories from filename.
                new_file = file.split("/")[-1]

                # Getting the destiny URL file.
                dir_destiny = "s3://" + bronze_bucket + "/" + processed_dir + new_file

                self.bucket_conn.copy_object(
                    source_bucket_key=source_obj_url,
                    dest_bucket_key=dir_destiny
                )

                # Verify that the file has been successfully copied to the destination directory.
                if self.bucket_conn.check_for_key(key=f"{processed_dir}{new_file}", bucket_name=bronze_bucket):

                    # Deleting the records already treated.
                    self.bucket_conn.delete_objects(bucket=bronze_bucket, keys=file)

                    # Log success message.
                    #log.info(f"File {new_file} has been saved to destination and removed from source.")

                else:

                    # Log error message.
                    logging.error(
                        f"File {new_file} is not in the destination, so it has not been removed from the source."
                    )

            # Log message.
            logging.info("Records have been saved to the list.")

            return valid_dicts

        except Exception as error:

            # Log error message.
            logging.critical(error)

            raise ValueError("Unable to get the data. Error:", error)

    def apollo_processed_at(self, data: list) -> list:
        """
        Description:
            Add value corresponding to timestamp from recording.

        Args:
            data: Data that will receive the timestamp marking.

        Returns:
            List with the timestamp values for all rows to be recorded into the parquet file.
        """

        for i in range(len(data)):

            # Add timestamp value.
            data[i]["apollo-processed-at"] = str(dt.utcnow())

        return data
