import json

class Json:

    @staticmethod
    def saveJson(data: dict or str, filename: str) -> None:

        # Saving data to a JSON file.

        with open(filename, "w") as f:
            json.dump(obj=data, fp=f, indent=4)

    @staticmethod
    def loadJson(filename: str) -> dict:

        # Loading data from a JSON file.

        with open(filename) as data:
            return json.load(fp=data)

    @staticmethod
    def fromDict(data: dict) -> str:

        # Serializing a dictionary.

        return json.dumps(obj=data)

    @staticmethod
    def toDict(data: str) -> dict:

        # Deserializing a dictionary.

        return json.loads(data)
