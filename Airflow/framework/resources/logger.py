import logging

class Logger:

    def __int__(self, logger_level=logging.INFO):
        logging.basicConfig(
            format="%(asctime)s-%(levelname)s-%(message)s", datefmt="%d-%m-%Y %H:%M:%S", level=logger_level
        )

    def _logger(self):
        return logging.getLogger()