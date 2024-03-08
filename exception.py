class CustomException(Exception):
    def __init__(self, err_):
        # for key, value in kwargs.items():
        #     setattr(self, key, value)
        
        self.msg_err = self.generate_meg_err(err_)
        
    def __iter__(self):
        return self
    
    def __next__(self):
        return next(self.msg_err)
    
    def generate_meg_err(self, err_):
        for i in  range(len(err_)):
            # msg_err = f"Filename: '{err_[i]['full_path']}' Status: '{err_[i]['status']}' Error: '{err_[i].get('errors')}'"
            # if err_[i]['status'] == 'success':
            #     self.n += 1
            msg_err = err_[i].get('errors')
            yield msg_err