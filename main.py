import argparse
import logging
from datetime import datetime, timedelta
from run_batch import convert_2_file
from exception import CustomException

class start_project(convert_2_file):
    def __init__(self):
        
        self.setup_folder()
        self.setup_log()
        try:
            date = datetime.today()
            logging.info(f"Start Run Batch Date: {date.strftime('%Y-%m-%d')}")
            super().__init__(date=date)
        except CustomException as errors:
            
            logging.error("Error Exception")
            
            while True:
                try:
                    msg_err = next(errors)
                    logging.error(msg_err)
                except StopIteration:
                    break
        finally:
            logging.info(f"Stop Batch Date {date.strftime('%Y-%m-%d')}\n")
        self.clear_folder()
            
if __name__ == "__main__":
    start_project()
    