import boto3
from botocore.exceptions import ClientError
import psycopg2
import logging

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from logger import Logger

load_logging()
logger = logging.getLogger('base')

redshift_client = boto3.client("redshift", region_name="us-east-1")

class Redshift:
    def __init__(self, database: str, host: str, port: str, user: str, password: str, aws_key_id: str, aws_key_password: str):

        self.database = database
        self.host = host
        self.port = port
        self.user = user
        self.password = password

        self.key_id = aws_key_id
        self.key_password = aws_key_password

        # starts with none with means that there is no database associated.
        self.conn = None
        self.cursor = None

    def _get_connection(self):
        """ Connect with the redshift warehouse

        Returns:
            obj: connection object
        """

        try:
            conn = psycopg2.connect(
                f"dbname={self.database} "
                f"host={self.host} "
                f"port={self.port} "
                f"user={self.user} "
                f"password={self.password}")
        except Exception as e:
            err = " Problem while get_connection on"\
                  " Database: " + self.database + "\r Error: " + str(e)
            logger.error(err)
            raise Exception(err)

        return conn

    def get_database(self):
        """ Retrieves the current holding database
        """

        return self.database

    def database_exists(self):
        """ Verify if the database exists
        """

        # get the connection
        try:
            self.conn = self._get_connection()
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor = self.conn.cursor()
        except :
            return False

        return True

    def table_exists(self, table_name: str):
        """ Check if the table exists in Redshift.

        Args:
            table_name (str): Name of the table that will be consulted.

        Returns:
            true  - if the table exists
            false - otherwise
        """

        try:
            self.cursor.execute(f"SELECT * from {table_name}")
        except:
            return False

        return True

    def run_sql(self, sql_command: str):
        """ Run sql command on redshift.

        Args:
            sql_command (str): sql like query that will be executed.
        """

        try:
            self.cursor.execute(sql_command)
        except Exception as e:
            err = " Error on runnin the following query: "\
                   + sql_command + " Error message:" + str(e) +\
                  " [END_ERROR] "
            logger.error(err)
            raise Exception(err)

    def load_data(
        self, data_location: str, table_name: str, format="PARQUET"):
        """ Load data from s3 into redshift temporary table.

        Args:
            data_location (str): S3 path of the data
            table_name (str): Name of the table where the data will be saved.
            format (str, optional): Format of the source data.
                Defaults to "PARQUET".
        """

        sql = f"COPY {table_name} FROM '{data_location}'"\
              f"CREDENTIALS 'aws_access_key_id={self.key_id};"\
              f"aws_secret_access_key={self.key_password}' {format}"
        try:
            self.run_sql(sql)
        except Exception as e:
            err = " Problem while loading table (" + table_name + \
                  ") from: " + data_location +" Error message: " + str(e)+\
                  " [END_ERROR] "
            logger.error(err)
            raise Exception(err)

    def create_table(
        self, table_name: str, col_name_type: dict, primary_key: str):
        """ Create a table inside a database

        Args:
            table_name (str): table name
            col_name_type (dict): dictionary with the columns and types
            primary_key (list): tables primary_keys
        """

        try:
            types = ""
            for key in col_name_type:
                types += f'"{key}" {col_name_type[key]}, '

        except Exception as e:
            err = " Problem while creating the types dictionary"\
                  " while creating tables.\t Coluns types: "\
                   + col_name_type + "\t Error: " + str(e)
            logger.error(err)
            raise Exception(err)

        if primary_key == []:
            sql = f"CREATE TABLE {table_name} ({types[:-2]})"
        else:
            pk = ",".join(primary_key)
            sql = f"CREATE TABLE {table_name} ({types}PRIMARY KEY ({pk}))"

        try:
            self.run_sql(sql)
        except Exception as e:
            err = " Problem while creating tables."\
                  " Error message: " + str(e)+" [END_ERROR] "
            logger.error(err)
            raise Exception(err)

    def create_database(self):
        """ Create database inside redshift
        """

        database = self.database
        # create a connection with the database
        if not self.database_exists():
            err = " It was not possible to connect with the"\
                  " database.\t Verify if the access, VPC, security"\
                  " groups."
            logger.error(err)
            raise Exception(err)

        sql = f"CREATE DATABASE {database}"
        try:
            self.run_sql(sql)
        except Exception as e:
            err = "Problem while creating database."\
                  " Error message: " + str(e)+" [END_ERROR] "
            logger.error(err)
            raise Exception(err)

        # check the connection with the new database
        if not self.database_exists():
            err = "It was not possible to connect with the"\
                  " database: " + database + "\t Verify if the access,"\
                  " VPC, securrity groups."
            logger.error(err)
            raise Exception(err)

    def delete(self, table_name: str):
        """ Drop tables. Mainly used to drop temporary tables

        Args:
            table_name (str): Name of the table that will be dropped.
        """

        sql = f"DROP TABLE {table_name}"

        try:
            self.run_sql(sql)
        except Exception as e:
            err = "Problem while dropping table in database."\
                  "\t Table:" + table_name + "\t Database:" + \
                  self.database + " Error message: " + str(e) + \
                  " [END_ERROR] "
            logger.error(err)
            raise Exception(err)

    def unload(self, sql_query: str, s3_taget: str,
               format="parquet", partition=None):
        """ Save the table into s3

        Args:
            sql_query (str): query the data that will be saved.
            s3_taget (str): S3 bucket where the data will be saved.
            format (str, optional): Save format. Defaults to "parquet".
            partition ([type], optional): Colname. Defaults to None.
        """

        # add an extra single opening (" ' ") to the sql
        query = sql_query.replace("'", "''")
        sql = f"UNLOAD ('{query}') to '{s3_taget}' "\
              f"CREDENTIALS 'aws_access_key_id={self.key_id};"\
              f"aws_secret_access_key={self.key_password}' {format}"

        if partition:
            sql += f" PARTITION by ({partition})"

        try:
            self.run_sql(sql)
        except Exception as e:
            err = "Problem while unloading data to s3."\
                  "Error message: " + str(e)+" [END_ERROR] "
            logger.error(err)
            raise Exception(err)

    def update(self, table_name: str, primary_key: list,
               col_name_type: dict, sql_query: str):
        """ Uppsert table with data

        Args:
            table_name (str): Name of the target table that will be updated.
            primary_key (list): List of primary key
            col_name_type (dict): Dictionary with the columns and their types.
            sql_query (str): sql to update table.
        """
        tmp = f"tmp_{table_name}_tmp"

        try:
            types = ""
            keywords = ""
            values = ""
            for key in col_name_type:
                types += f'"{key}"="{tmp}"."{key}",'
                keywords += f'"{key}",'
                values += f'"{tmp}"."{key}",'

            # removes extra ","
            types = types[:-1]
            keywords = keywords[:-1]
            values = values[:-1]

        except Exception as e:
            err = "Problem while creating the types dictionary."\
                  "\t Coluns types: " + col_name_type + \
                  "\t Error: " + str(e)
            logger.error(err)
            raise Exception(err)

        # update when have the same fields for pk
        sql = sql_query.replace("\\", "")
        sql_update = f"UPDATE {table_name} "\
                     f"SET {types} FROM ({sql}) {tmp} WHERE"

        # insert when have different values
        sql_insert = f"INSERT INTO {table_name} ({keywords})"\
                     f" SELECT {values} FROM ({sql}) {tmp} WHERE"

        if primary_key ==[]:
            sql_insert += " 1=1"
        else:
            for pk in primary_key:
                sql_update += f' "{tmp}"."{pk}"="{table_name}"."{pk}" AND'
                sql_insert += f' "{tmp}"."{pk}" NOT IN '\
                              f'( SELECT "{pk}" FROM "{table_name}") AND'
            # removes extra AND
            sql_update = sql_update[:-3]
            sql_insert = sql_insert[:-3]

            try:
                self.run_sql(sql_update)
            except Exception as e:
                err = "Problem while updating data."\
                    "\t SQL: " + sql_update + "\t Error: " + str(e)
                logger.error(err)
                raise Exception(err)

        try:
            self.run_sql(sql_insert)
        except Exception as e:
            err = "Problem while inserting data."\
                  "\t SQL: " + sql_insert + "\t Error: " + str(e)
            logger.error(err)
            raise Exception(err)

class Dynamo:
    """
    Description: Base class with methods to interact with the DynamoDB AWS
    service. The methods from this class are generic and expected to be
    herded from child custom classes.

    Args:
        table(str): Name of the DynamoDB table.

    Attributes:
        table(DynamoDB.Table): An child resource from the _dynamo attribute
            which points to the table specified in the initializer.
        logger(logging.Logger): Base logger from the custom loggers in Infra.
        _console_logger(logging.Logger): Console logger from the
            custom loggers in Infra.
        _dynamo(boto3.sessions.Session.resource): Resource from boto3
            which points to the dynamodb resource in us-east-1 region.
    """

    def __init__(self, table: str) -> None:
        self._dynamo = boto3.resource('dynamodb', region_name='us-east-1')
        self.table = self._dynamo.Table(table)
        self.logger = logging.getLogger('base')
        # pylint: disable=unused-private-member
        self._console_logger = logging.getLogger('console')

    def insert_data(self, data: dict) -> None:
        """
        Description: Inserts data in to the DynamoDB `table`.

        Args:
            data(dict): An dictionary wich each key is a column and its
                associated valued is inserted in the corresponding column
                in the DynamoDB `table`.
        """

        try:
            response = self.table.put_item(
                TableName=self.table,
                Item=data
                )
        except Exception as error:
            self.logger.error(
                "Failed to insert data into dynamo: %s", error
                )
            raise

        return response

    def _get_data(self, key: dict) -> dict:
        """
        Description: A protected method to get data from the DynamoDB `table`.

        Args:
            key(dict): An dict with the PartitionKey:SortKey to search for
                data in the DynamoDB `Table`.

        Returns:
            An response (`dict` object) from the DynamoDB service.
        """

        try:
            response = self.table.get_item(Key=key)
        except ClientError as error:
            self.logger.error("Failed to retrieve data: %s", error.response["Error"]["Message"])
        return response
