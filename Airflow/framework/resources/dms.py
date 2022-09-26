import boto3

dms_client = boto3.client("dms", region_name="us-east-1")

class Dms:
    @staticmethod
    def call_task(task_arn: str):
        """
        Call a replication task by arn

        Args:
            task_arn (str): unique aws identifier of the task

        Returns:
            dict: http response
        """

        try:
            response = dms_client.start_replication_task(
                ReplicationTaskArn=task_arn,
                StartReplicationTaskType='reload-target'
            )
        except Exception as e:
            logger.error("Failed to call tasks: %s", e)
            raise Exception(e)

        return response
