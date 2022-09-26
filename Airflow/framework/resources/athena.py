# Not working yet!

import boto3

athena_client = boto3.client("athena", region_name="us-east-1")

class Athena:

    def create_table(self, col_name_type: dict, table_name: str, database: str, catalog: str, s3_data_location: str, query_output_location: str):
        """
        Creates a table that will reflect data on Athena.

        Args:
            col_name_type (dict): dictionary with column name : type.
            table_name (str): name of the table.
            database (str): database name.
            catalog (str): catalog name.
            s3_data_location (str): s3 location of the data
            query_output_location (str): s3 location to save the query results.
        """
        try:
            types = ""
            for key in col_name_type:
                types += f"`{key}` {col_name_type[key]},"
            types = types[:-1]
        except Exception as e:
            err = f"Problem with the data types of athena config file Coluns types: {col_name_types}, error{e}"
            logger.error(err)
            raise Exception(err)

        sql = f"CREATE EXTERNAL TABLE `{table_name}`({types}) "\
            "ROW FORMAT SERDE " \
            "'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' " \
            "STORED AS INPUTFORMAT " \
            "'org.apache.hadoop.hive.ql.io.parquet."\
            "MapredParquetInputFormat' " \
            "OUTPUTFORMAT " \
            "'org.apache.hadoop.hive.ql.io.parquet"\
            ".MapredParquetOutputFormat' " \
            "LOCATION " \
            f"'{s3_data_location}' " \
            "TBLPROPERTIES ('CrawlerSchemaDeserializerVersion'='1.0', " \
            "'CrawlerSchemaSerializerVersion'='1.0', " \
            "'classification'='parquet', " \
            "'compressionType'='none', " \
            "'typeOfData'='file')"

        self.run_query(
            sql=sql,
            database=database,
            catalog=catalog,
            query_output_location=query_output_location
        )

    @staticmethod
    def run_query(sql: str, database: str, catalog: str, query_output_location: str):
        """
        Run a query into athena.

        Args:
            sql (str): presto sql
            database (str): name of the database of athena where the data is.
            catalog (str): name of the catalog of the database.
            query_output_location (str): s3 location to save the query results.
        """

        try:

            athena_client.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={
                    "Database": database,
                    "Catalog": catalog
                },
                ResultConfiguration={
                    "OutputLocation": query_output_location,
                    "EncryptionConfiguration": {
                        "EncryptionOption": "SSE_S3"
                    }
                }
            )

        except Exception as e:
            logger.error(
                "It was not possible to run the query. Query: %s, error: %s", sql, e
            )
            raise Exception(
                f"It was not possible to run the query. Query: {sql}. Error: {e}."
            )

    def table_exists(self, table_name: str, database: str, catalog: str):
        """
        Check if the table exists.

        Args:
            table_name (str): Name of the table.
            catalog (str): Name of the catalog of the database.
        """

        try:
            request = athena_client.list_table_metadata(
                CatalogName=catalog,
                DatabaseName=database
            )

            name_list = [i["Name"] for i in request["TableMetadataList"]]

        except:
            return False

        if table_name in name_list:
            return True

        return False