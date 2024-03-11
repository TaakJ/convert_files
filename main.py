import logging
from run_batch import convert_2_file
from setup import setup_parser, setup_folder, setup_log, clear_folder
from exception import CustomException
from datetime import datetime

class setup_project(convert_2_file):
    def __init__(self):
        
        setup_folder()
        setup_log()
        try:
            date = datetime.now()
            batch_date = setup_parser().param.batch_date
            logging.info(f"Start Run Batch Date: {date}")
            super().__init__(batch_date=batch_date, date=date)
            
        except CustomException as errors:
            logging.error("Error Exception")
            while True:
                try:
                    msg_err = next(errors)
                    logging.error(msg_err)
                except StopIteration:
                    break
        finally:
            logging.info(f"Stop Batch Date {date}\n") 
            ''
        clear_folder()
        
if __name__ == "__main__":
    setup_project()
    