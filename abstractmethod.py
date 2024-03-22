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

class verify:
    def open_files(self):
        self.log = [{"mode": "write"}]
        for key in self.log:
            key.update({"mode": "write"})

class convert_files(call_logging):

    def __init__(self, content):
        super().__init__()
        # self.content = content
        # print(self.content)
        # self.page = 0
        self.update()
        # self.insert()
        # self.delete()

    def _log_setter(self, log):
        self._log = log

    def update(self):
        
        # self.content[self.page] = 1
        # self.page += 1
        self._log_setter(self.a)

    def insert(self):
        self.open_files()

    def delete(self):
        print(self.log)


class call(convert_files, verify):
    pass


s = call(content={})
