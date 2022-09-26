import logging
import boto3
import botocore.config
import json
import base64
import re
import yaml
#from Logger.loader import load_logging

#load_logging()
logger = logging.getLogger("base")

class LambdaFunc:

    def __init__(self, lambda_name: str, payload: dict, return_response: bool = False):
        self.config_file = yaml.load("Infra/aws/configs/lambda_config.yaml")
        self.lambda_name = lambda_name
        self.payload = payload
        self.return_response = return_response

    def lambda_obj(self) -> boto3.client:
        """
        This func aims to create a lambda client obj with the correct configuration. If you need a different
        configuration when invoke a Lambda, just change the variables above.

        :return: a lambda object
        """

        cfg = botocore.config.Config(
            retries=self.config_file["retries"],
            read_timeout=self.config_file["read_timeout"],
            connect_timeout=self.config_file["connect_timeout"],
            region_name=self.config_file["region_name"]
        )

        lambda_client = boto3.client("lambda", config=cfg)

        return lambda_client


    def call_lambda(self):
        """
        Invoke lambda function

        :return: [bool]: True case succeed, False case not succeed
        """

        # Getting the lambda client
        lambda_client = self.lambda_obj()

        try:

            lambda_response = lambda_client.invoke(
                FunctionName=self.lambda_name,
                InvocationType="RequestResponse",
                LogType="Tail",
                Payload=bytes(json.dumps(self.payload), encoding="utf8")
            )

            lambda_error = lambda_response.get("FunctionError", "null")

            if lambda_response["StatusCode"] != 200 or lambda_error != "null":

                message = f"Status Code: {lambda_response['StatusCode']}.\n \
                Function Error: {lambda_response['FunctionError']}.\n \
                Log: {base64.b64decode(lambda_response['LogResult'].encode('ascii')).decode('utf-8')}.\n"

                logger.error("Invoke Lambda failed!", message)

                raise Exception(message)

            else:

                return True, lambda_response

        except Exception as error:

            raise Exception("Invoke Lambda failed! Error:\n{}".format(error))

# TODO: get original file from backup to understand the line 101

#     def get_memory(self, response: dict) -> tuple:
#         """
#         Get the amount of memory requested and utilized in an AWS Lambda invoke.
#
#         :arg:
#             Response(dict): Response from AWS Lambda.
#
#         :return:
#             size(int): Memory size requested for the lambda invoke.
#             used(int): Memory utilized for the lambda invoke.
#         """
#
#         size_pattern = 'Memory Size:\s(\d+)'
#         used_pattern = 'Memory Used:\s(\d+)'
#         log_result = self.log_decoder(response)
#         try:
#             size = re.search(size_pattern, log_result).group(1)
#             used = re.search(used_pattern, log_result).group(1)
#             size = int(size)
#             used = int(used)
#         except Exception as error:
#             logger.warning("Failed to find amount of memory: %s", error)
#         return size, used
