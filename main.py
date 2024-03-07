import argparse
import logging
from datetime import datetime, timedelta
from run_batch import convert_2_file
from exception import CustomException

class start_project(convert_2_file):
    def __init__(self):
        
        self.setup_log()
        # self.backup_folder()
        # self.clear_folder()
        # self.setup_folder()
        
        try:
            fix_date = datetime(2024, 2, 26)
            date = datetime.today()
            days = (date - fix_date).days
            date = date - timedelta(days=days)
            
            logging.info("Start Project")
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
        finally:
            logging.info("End Project")
            
if __name__ == "__main__":
    start_project()
    