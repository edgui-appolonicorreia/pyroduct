from airflow.models import Variable
from datetime import datetime, timedelta

class DataAmount:

    @staticmethod
    def _amount_of_data(amount_of_data: str) -> str:
        """"""

        # Checking if the 'amount_data' var it's available
        if amount_of_data in ["few", "medium", "huge"]:

            return amount_of_data

        else:

            raise ValueError(f"Amount of Data invalid! Error value: {amount_of_data}")

class General:

    # Defining a datetime format.
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    @staticmethod
    def getNextDate(date: datetime, timeInterval: dict, dateFormat: str = DATETIME_FORMAT) -> str:
        """
        Creating a method to capture the date of the day following the date of entry.

        Args:
            date: It is the base date that you want to use.
            dateFormat: It is the format to which you want to convert the base date.
            timeInterval: Defines the amount of time that must be given between the current date and the next date.

        Returns:
            nexDate: It is the base date plus the number of days that have progressed.
        """

        # Converting the datetime variable to a string format.
        nextDate = datetime.strptime(date, dateFormat)

        # Converting dictionary values to integer.
        for k in timeInterval.keys():
            timeInterval[k] = int(timeInterval[k])

        # Adding another day to the current date.
        nextDate = nextDate + timedelta(
            days=timeInterval["days"],
            seconds=timeInterval["seconds"],
            microseconds=timeInterval["microseconds"],
            milliseconds=timeInterval["milliseconds"],
            minutes=timeInterval["minutes"],
            hours=timeInterval["hours"],
            weeks=timeInterval["weeks"]
        )

        return nextDate.strftime(dateFormat)

    @staticmethod
    def itsAPastDate(dateTime: str) -> bool:
        """
        Creating method to validate if a date is less than or equal to the current date

        Args:
            dateTime: It is the date to be verified.

        Returns:
            Returns a Boolean value indicating whether the date is less than or equal to the current date.
        """

        return datetime.strptime(dateTime, General.DATETIME_FORMAT) < datetime.now()

    @staticmethod
    def update_interval(dagNameParams: str):

        # Capturing the Airflow variable that stores the DAG metadata.
        params = Variable.get(dagNameParams, default_var=False, deserialize_json=True)

        # Capturing the variable that indicates the time span that should be applied.
        timeInterval = params["time_interval"]

        # Changing the due date to the start date.
        params["params"]["start_date"] = params["params"]["end_date"]

        # Generating the next end date.
        params["params"]["end_date"] = General.getNextDate(
            date=params["params"]["end_date"],
            timeInterval=timeInterval
        )

        # Saving the updated variable as an Airflow variable.
        Variable.set(dagNameParams, value=params, serialize_json=True)
