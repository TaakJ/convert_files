import argparse
import logging
from datetime import datetime
from start import convert_2_file
from exception import CustomException

class start_project(convert_2_file):
    def __init__(self):
        
        # self.setup_log()
        # self.backup_folder()
        # self.clear_folder()
        # self.setup_folder()
        
        try:
            date = datetime.datetime(2024, 2, 27)
            logging.info("Start Project")
            date = datetime.now()
            super().__init__(date=date)
            
        except CustomException as err:  
            logging.error("Error Exception")     
            err_ = iter(err)
            while True:
                try:
                    msg_err = next(err_)
                    logging.error(msg_err)
                except StopIteration:
                    break
            logging.error(f"file found count {err.n} status: successed")
        
        finally:
            logging.info("End Project")
            
if __name__ == "__main__":
    start_project()
    