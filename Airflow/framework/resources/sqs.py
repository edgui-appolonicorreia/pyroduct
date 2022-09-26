import logging
import boto3
from logger.loader import load_logging

load_logging()
logger = logging.getLogger('base')

sqs_client = boto3.client("sqs", region_name="us-east-1")

class Sqs:

    @staticmethod
    def sqs_messages_count(sqs_name: str):
        """ Count the amount of messages in a sqs queue.

        Args:
            sqs_name (str): name of the sqs.

        Returns:
            [int]: number of messages.
        """

        try:
            sqs_response = sqs_client.get_queue_attributes(
                QueueUrl=sqs_name,
                AttributeNames=["ApproximateNumberOfMessages"]
            )
            total_messages = \
                int(sqs_response["Attributes"]["ApproximateNumberOfMessages"])

        except Exception as e:
            logger.error(
                "Failed to read SQS queue: %s", e) 
            raise Exception("Error while reading sqs.\n{}".format(e))

        return total_messages
