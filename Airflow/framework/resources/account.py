# Check: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html

import boto3

class StatusAccount:

    def __init__(self):
        self.client = boto3.client("sts")

    def get_user_id(self):
        """
        :return: [str]: the user ID
        """

        user_id = self.client.get_caller_identity()["UserId"]

        return user_id

    def get_account_id(self):
        """
        :return: [str]: the account ID
        """

        account_id = self.client.get_caller_identity()["Account"]

        return account_id

    def get_arn(self):
        """
        :return: [str]: the account ID
        """

        arn_id = self.client.get_caller_identity()["Arn"]

        return arn_id
