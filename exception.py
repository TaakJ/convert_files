class CustomException(Exception):
    def __init__(self, **kwargs: dict):
        self.__dict__.update(kwargs)
        
        for key, value in self.__dict__.items():
            setattr(self, key, value)
        
        self.msg_err = self.generate_meg_err()
    
    def __iter__(self):
        return self

    def __next__(self):
        return next(self.msg_err)

    def generate_meg_err(self):
        for i in  range(len(self.errors)):
            msg_err = f"Path::{self.errors[i]['source']} - Function::{self.errors[i]['function']} - Status::{self.errors[i]['status']} - Error::{self.errors[i].get('errors')}"
            yield msg_err