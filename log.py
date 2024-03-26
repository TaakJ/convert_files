from abc import ABC, abstractmethod

class call_logging(ABC):
    def __init__(self):
        self._log = []

    @property
    def log(self):
        return self._log

    @log.setter
    def log(self, log):
        self._log_setter(log)

    @abstractmethod
    def _log_setter(self, log):
        pass