import logging as log
import pandas as pd
from io import BytesIO
from airflow.providers.amazon.aws.hooks import s3

class Parquet:
    """
        Description:
            Class to handle with parquet files.
    """

    # Log configs
    log.basicConfig(format="%(process)d-%(levelname)s-%(message)s", level=log.INFO)

    @staticmethod
    def check_parquet_file(bucket_conn: s3, bucket_name: str, dir: str, parquet_name: str) -> bool:
        """
        Description:
            Return true if file exists and false if not.

        Args:
            bucket_conn: object type s3. The connection with S3 open previously by another class.
            bucket_name: Name of the Bucket in S3.
            dir: Name of the directory in S3 bucket.
            parquet_name: Name of parquet file to check.

        Returns:
            A boolean value or raise an error.
        """

        try:

            # Getting all file names in the bucket.
            files = bucket_conn.list_keys(bucket_name=bucket_name, prefix=dir)

            # Check if the files exists in the bucket.
            if f"{dir}{parquet_name}" in files:

                # Log message.
                log.info(f"File '{parquet_name}' is in the directory.")

                # Status if exists.
                return True

            else:

                # Log message.
                log.info(f"File '{parquet_name}' is not in the directory.")

                # Status if it does not exist.
                return False

        except Exception as error:

            # Status reporting the error.
            log.critical(error)

    @staticmethod
    def s3obj_to_df(obj) -> pd.DataFrame:
        """
        Description:
            Convert s3 object (parquet) into parquet.

        Args:
            obj: S3 object to converts. This object needs to be in parquet format.

        Returns:
            Dataframe.
        """

        try:

            # Reads the object.
            obj = obj.get()["Body"].read()

            # Transform the object into bytes.
            parquet_file = BytesIO(obj)

            # Convert the object into pandas dataframe.
            dataframe = pd.read_parquet(parquet_file, engine="pyarrow")

            # Log message.
            log.info("Object has been converted to dataframe.")

            # Return the dataframe.
            return dataframe

        except Exception as error:

            # Status reporting the error.
            log.critical(error)

    @staticmethod
    def parquet_to_df(bucket_conn: s3, bucket_name: str, dir: str, parquet_name: str) -> pd.DataFrame:
        """
        Description:
            Converts parquet into dataframe.

        Args:
            bucket_conn: object type s3. The connection with S3 open previously by another class.
            bucket_name: Name of the Bucket in S3.
            dir: Name of the directory in S3 bucket.
            parquet_name: Name of the parquet file.

        Returns:
            Dataframe.
        """

        try:

            # Gets the object from S3.
            obj = bucket_conn.get_key(key=f"{dir}{parquet_name}", bucket_name=bucket_name)

            # Convert the object into pandas dataframe.
            dataframe = Parquet.s3obj_to_df(obj=obj)

            # Log message.
            log.info("The parquet was converted to dataframe.")

            # Return dataframe.
            return dataframe

        except Exception as error:

            # Status reporting the error.
            log.critical(error)

    @staticmethod
    def df_to_parquet(data: dict, parquet_name: str) -> bool:
        """
        Description:
            Converts dataframe in parquet.

        Args:
            data: Data in dict format.
            parquet_name: Name of the parquet file.

        Returns:
            A True value or raise an error.
        """

        try:

            # Converting the dataframe dict into a dataframe type
            df_final = pd.DataFrame(data)

            # Converts dataframe in parquet.
            df_final.to_parquet(fname=parquet_name, engine="pyarrow", index=False)

            # Log message.
            log.info("The dataframe was converted to parquet.")

            return True

        except Exception as error:

            # Status reporting the error.
            log.critical(error)

    @staticmethod
    def df_to_parquet_to_s3(bucket_conn: s3, bucket_name: str, dir: str, data: dict, parquet_name: str) -> bool:
        """
        Description:
            Converts dataframe in parquet and saves in S3 bucket.

        Args:
            bucket_conn: object type s3. The connection with S3 open previously by another class.
            bucket_name: Name of the Bucket in S3.
            dir: Name of the directory in S3 bucket.
            data: Data in dict format.
            parquet_name: Name of the parquet file.

        Returns:
            A True value or raise an error.
        """

        try:

            # Converts dataframe into parquet.
            Parquet.df_to_parquet(data=data, parquet_name=parquet_name)

            # Saves parquet file in S3 bucket.
            bucket_conn.load_file(
                filename=parquet_name,
                key=f"{dir}{parquet_name}",
                bucket_name=bucket_name,
                replace=False
            )

            # Log message.
            log.info("Parquet was successfully saved into S3 bucket!")

            return True

        except Exception as error:

            # Status reporting the error.
            log.critical(error)

    @staticmethod
    def write_on_file(bucket_conn: s3, bucket_name: str, dir: str, data: dict, parquet_name: str) -> bool:
        """
        Description:
            Writes records in the parquet file from Bucket S3.

        Args:
            bucket_conn:
            bucket_name: Name of the Bucket in S3.
            dir: Name of the directory in S3 bucket.
            data: Dict to be used to append records in to Parquet file.
            parquet_name: Name of the parquet file.

        Returns:
            A True value or raise an error.
        """

        try:

            # Gets object from S3 bucket.
            obj = bucket_conn.get_key(key=f"{dir}{parquet_name}", bucket_name=bucket_name)

            # Convert dict into pandas dataframe to append.
            dataframe_dict = pd.DataFrame(data)

            # Convert the object into pandas dataframe.
            dataframe = Parquet.s3obj_to_df(obj=obj)

            # List with columns from S3 parquet file.
            df_columns = list(dataframe.columns)

            # List with columns from dict to append.
            df_dict_columns = list(dataframe_dict.columns)

            # Check if the columns from both dataframes are equal. If equal:
            if sorted(df_columns) == sorted(df_dict_columns):

                # Append.
                dataframe = dataframe.append(dataframe_dict, ignore_index=True)

                # Converts pandas dataframe to parquet file.
                dataframe.to_parquet(parquet_name, index=False, engine="pyarrow")

                # Save the file.
                bucket_conn.load_file(
                    filename=parquet_name,
                    key=f"{dir}{parquet_name}",
                    bucket_name=bucket_name,
                    replace=True
                )

                # Log message.
                log.info("Recording has been completed successfully.")

                return True

            else:

                # Log message.
                log.error("The columns are incompatible. Recording failed.")

                return False

        except Exception as error:

            # Log message.
            log.critical(error)
