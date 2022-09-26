import logging
import ast
import boto3
from logger.loader import load_logging

load_logging()
logger = logging.getLogger('base')

secret_client = boto3.client('secretsmanager',  region_name="us-east-1")

class Secrets:

    @staticmethod
    def get_secret(secret_location: str):
        """ Access amazon secret store and retrives the secret in the
            given location.

        Args:
            secret_location (str): Name of the secret on aws

        Returns:
            Dictionary with the secret.
        """

        try:
            credential_key = secret_client.get_secret_value(
                SecretId=secret_location)["SecretString"]
        except Exception as e:
            logger.error(
                "Failed to retrieve secret from vault: %s", e)
            raise Exception(e)

        return ast.literal_eval(credential_key)
