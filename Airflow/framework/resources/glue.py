# Not working yet!

import boto3
import awswrangler as wr

class Glue:

    def __init__(self, database: str, table: str, stage_bucket: str):
        """ Glue class contructor.

        Args:
            database (str): name of the database.
            table (str): name of the table.
            stage_bucket (str): name of the bucket were the data is on.
        """

        self.glue_client = boto3.client("glue", region_name="us-east-1")
        self.db = database
        self.table = table
        self.crawler_name = f"{database}-{table}"
        self.s3 = f"s3://{stage_bucket}/{database}/{table}"

    def crawler_exists(self):
        """ Verify if the crawler existis

        Returns:
            [bool]: True if the crawler exists, false if not
        """
        try:
            glue_client.get_crawler(Name=self.crawler_name)
        except Exception:
            return False
        return True

    def create_crawler(self, iam_role: str):
        """ Creates a new crawler.

        Args:
            iam_role (str): IAM arn of role that the crawler will assume.

        Raises:
            Exception: If the crawler cound not be created raise error.
        """
        try:
            glue_client.create_crawler(
                Name=self.crawler_name,
                Role=iam_role,
                DatabaseName=self.db,
                Description='string',
                Targets={
                    'S3Targets': [
                        {
                            'Path': self.s3,
                        },
                    ]
                },
                SchemaChangePolicy={
                    'UpdateBehavior': 'LOG',
                    'DeleteBehavior': 'LOG'
                },
                RecrawlPolicy={
                    'RecrawlBehavior': 'CRAWL_NEW_FOLDERS_ONLY'
                }
            )
        except Exception as e:
            logger.error(
                "Failed to create the Crawler: %s. Error: %s", self.crawler_name, e)
            raise Exception(
                "It was not possible to create the"
                f" Crawler '{self.crawler_name}'."
                f"Error:{e}.")

    def database_exists(self):
        """ Verify if the database already exists.

        Returns:
            [bool]: True if exists, false otherwise.
        """
        try:
            glue_client.get_database(Name=self.db)
        except Exception:
            return False
        return True

    def create_database(self):
        """ Creates a glue database

        Raises:
            Exception: Raise exception if database does not create.
        """
        try:
            glue_client.create_database(DatabaseInput={'Name': self.db})
        except Exception as e:
            logger.error(
                "Failed to create the database %s. error: %s", self.db, e)
            raise Exception(
                f"It was not possible to create the database '{self.db}'."
                f"Error:{e}.")

    def start_crawler(self):
        """ Start a existing crawler.

        Raises:
            Exception: If the crawler does not execute correctly.
        """
        try:
            glue_client.start_crawler(Name=self.crawler_name)
        except Exception as e:
            logger.error(
                "Failed to start crawler %s. error: %s", self.crawler_name, e)
            raise Exception(
                f"It was not possible to start crawler'{self.crawler_name}'."
                f"\nError:{e}.")
