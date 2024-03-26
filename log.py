from abc import ABC, abstractmethod

class call_logging(ABC):
    def __init__(self):
        self._log = []

    @property
    def logging(self):
        return self._log

    @logging.setter
    def logging(self, log):
        self._log_setter(log)

    @abstractmethod
    def _log_setter(self, log):
        pass