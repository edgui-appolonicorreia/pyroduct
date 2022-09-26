import io
from datetime import datetime, timedelta
from dateutil import parser
import logging
import boto3
from logger.loader import load_logging

load_logging()
logger = logging.getLogger('base')
s3_client = boto3.client("s3", region_name="us-east-1")
s3_session = boto3.Session().resource('s3')

class S3():

    def __init__(self):

        super().__init__()

    @staticmethod
    def get_files_statitics(
        bucket: str, dataset: str, table: str, subfolder: str
    ):
        """ Gets the amount of data and files that will be processed.

        Args:
            bucket (str): bucket name
            dataset (str): dataset name
            table (str): table name
            subfolder (list(str)): the subfolders of table
        """

        size = 0
        total = 0
        for folder in subfolder:

            request = s3_client.list_objects(
                Bucket=bucket,
                Prefix=folder
            )
            try:
                # sometimes the response comes with data of the folder
                first_element = request["Contents"][0]["Key"].replace("/", "")
                if first_element == folder.replace("/", ""):
                    size_list = [
                        int(i["Size"]) for i in request["Contents"][1:]
                    ]
                else:
                    size_list = [int(i["Size"]) for i in request["Contents"]]
            except:
                # if it came to this except it means that there is no folder
                # or file in the path
                logger.warning("No folder of file in the path."
                              f"\t Bucket: {bucket} \t Prefix: {folder}"
                )

                return 0, 0

            size += sum(size_list)
            total += len(size_list)

        return total, size

    @staticmethod
    def get_key(bucket: str, folder: str, all_data=True):
        """ Get all the bucket keys inside one specific folder.

        Args:
            bucket (str): bucket name.
            folder (str): subfolder.
            all_data (bool): specify if you want to all the keys inside the
                folder(True) or just the child directorys(False). Default: True
        """

        try:
            keys = []
            for objects in s3_session.Bucket(bucket)\
                .objects.filter(Prefix=folder):
                keys.append(objects.key)
        except Exception as e:
            logger.error("Error while listing bucket: (%s) with the prefix: "
                        "(%s). Error: (%s)", bucket, folder, str(e))
            raise

        if all_data:
            return keys

        # get just the folder
        folders = []
        for key in keys:
            end = key.rsplit("/")[-1]
            if end == "":
                # it means that is a folder
                folders.append(key)
            else:
                folders.append(key.replace(end, ""))

        # removing duplicates
        folders = list(set(folders))

        return folders

    @staticmethod
    def get_buckets_to_process(
        s3_origin: str,
        s3_target: str,
        dataset: str,
        table: str,
        timestamp: str,
        target_table=None,
        all_files=False
    ):
        """ Look for the files that need to be processed

        Args:
            s3_origin (str): bucket with the data in a prior state
            s3_target (str): bucket that have the processed data
            dataset (str): dataset name
            table (str): table name
            timestamp (str): period of time to process
            target_table (str): name of the target table to compare, if none it
                will use table as target as well.
            all_files (bool): Return all the files that are different from
                target.
        """

        # dict that map the amount of seconds in each period
        timestamp_dict = {
            "M": 30*24*60*60,
            "D": 24*60*60,
            "H": 60*60
        }

        prefix = f"{dataset}/{table}"

        if not prefix.endswith('/'):
            prefix += '/'

        try:
            origin_request = []
            for objects in s3_session.Bucket(s3_origin)\
                .objects.filter(Prefix=prefix):
                origin_request.append(objects.key)
        except Exception as e:
            logger.error("Error while listing bucket: (%s) with the prefix: "
                        "(%s). Error: (%s)", s3_origin, prefix, str(e))
            raise

        if target_table:
            prefix_target = f"{dataset}/{target_table}"
        else:
            prefix_target = prefix

        if not prefix_target.endswith('/'):
            prefix_target += '/'
        try:
            target_request = []
            for objects in s3_session.Bucket(s3_target)\
                .objects.filter(Prefix=prefix):
                target_request.append(objects.key)
        except Exception as e:
            logger.warning(
                    " The target bucket: (%s) or prefix: (%s) may not exist"
                    " exception occurs: (%s)", s3_origin, prefix, str(e)
                )

        # verify if bucket target exist
        if len(target_request) == 0 and (not all_files):
            logger.warning(
                f"Target folder not found! "
                f"Target: {s3_target} "
                f"Prefix_target: {prefix_target} "
            )

            if not prefix.endswith('/'):
                prefix += '/'

            return [prefix]

        # getting values that are not in the origin
        originn, otime = _get_data_folders(origin_request)
        targett, ttime = _get_data_folders(target_request)

        origin_set = set(otime)
        target_set = set(ttime)

        logger.info(
            f"Origin: {s3_origin}/{prefix} {list(origin_set)} "
            f"Target: {s3_target}/{prefix_target} "\
            f"{list(target_set)}"
        )

        if all_files:
            # take all directorys that do not have a copy on target
            not_in_target = list(origin_set.difference(target_set))
        elif max(otime) <= max(target_set):
            # there is no data to process
            logger.info(
                "There is no data left to process"
                ", everything update!"
            )
            return []
        else:
            # get all the files olders
            not_in_target = []
            for i in origin_set:
                if i > max(target_set):
                    not_in_target.append(i)

        # get data today
        today = datetime.now()

        must_process = []
        # Check if the data is in the timestamp
        for i in not_in_target:
            # find the index of the value in the list
            index = otime.index(i)
            try:
                # get the senconds of the difference of the current day and
                # the folders day
                diff = (today - i).total_seconds()
                # get the minimus desirable time in second to process the
                # folder
                period = timestamp[-1]
                number = timestamp[:-1]
                timestamp_seconds = timestamp_dict[period] * float(number)
                if diff>= timestamp_seconds:
                    must_process.append(originn[index])
            except Exception as e:
                war = "There is a file not in the "\
                      "datalake supported pattern."\
                      "\t File:"+i+"\t Or another issue:"+str(e)
                logger.warning(war)

        logger.info(f"Process: {s3_origin}/{prefix} "\
            f"\n{list(must_process)}\n")

        return must_process

    @staticmethod
    def get_times_to_process(
        start_date: str,
        s3_bucket: str,
        time_step: str,
        owner: str,
        table: str
    ):
        """ Check if there is some timerange left to process, and process then.

        Args:
            start_date (str): First day
            s3_bucket (str): Bucket where tha data is located
            time_step (str): Frequency with the data will be processed
            owner (str): Departament of the table
            table (str): table_name
        """

        # dict that map the amount of seconds in each period
        timestamp_dict = {
            "M": 30*24*60*60,
            "D": 24*60*60,
            "H": 60*60
        }
        period = time_step[-1]
        number = time_step[:-1]
        timestamp_seconds = timestamp_dict[period] * float(number)

        prefix = f"{owner}/{table}"

        if not prefix.endswith('/'):
            prefix += '/'

        try:
            processed_times = []
            for objects in s3_session.Bucket(s3_bucket)\
                .objects.filter(Prefix=prefix):
                processed_times.append(objects.key)
        except Exception as e:
            logger.error("Error while listing bucket: (%s) with the prefix: "
                        "(%s). Error: (%s)", s3_bucket, prefix, str(e))
            raise


        _, times = _get_data_folders(processed_times)

        # list all the times between the start and now that should exist
        start_date = parser.parse(start_date)
        today = datetime.today()

        total_times = int(
            (today - start_date).total_seconds() / timestamp_seconds)

        date_list = []
        for i in range(total_times):
            if i == 0:
                date_list.append(
                    start_date + timedelta(seconds=timestamp_seconds)
                    )
            else:
                date_list.append(
                    date_list[i-1] + timedelta(seconds=timestamp_seconds)
                    )

        if times == []:
            return [i.strftime("%Y-%m-%d %X") for i in date_list]
        elif max(times) < max(date_list):
            return [
                i.strftime("%Y-%m-%d %X") for i in date_list if i>max(times)
            ]
        else:
            return []

def _get_data_folders(folders: list):
    """ Get the data timestamp of the folders on s3.

    Args:
        folders (list): Folders that the timestamp will be captured.
    """

    folders_timestamp = []
    sub_folders = []
    # get the subforders with data
    for i in folders:
        data_folder = i.split("/")[2]
        folder = "/".join(i.split("/")[:3])+"/"
        # validate if is a data format or other file
        try:
            convert_data = parser.parse(data_folder)
            sub_folders.append(folder)
            folders_timestamp.append(convert_data)
        except Exception as e:
            # this mean that has files that are not in the datalake
            # patter, and will not be used
            logger.warning(
                "There is a file not in the "\
                "datalake supported pattern."\
                "\t File:"+i+"\t Or another issue:"+str(e)
            )

    return sub_folders, folders_timestamp
