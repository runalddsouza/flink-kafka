import logging as log
import sys


class AppLogger:
    def __init__(self, path):
        self.log = log
        self.log_path = path
        self.log.root.handlers = []
        self.log.basicConfig(
            format="%(asctime)s - %(levelname)s - %(threadName)s - %(lineno)d - %(message)s",
            level=log.DEBUG,
            handlers=[log.FileHandler(self.log_path), log.StreamHandler(sys.stdout)])

    def get_logger(self):
        return self.log