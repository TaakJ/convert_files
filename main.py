import logging
from datetime import datetime, timedelta
from run_batch import convert_2_file
from setup import setup_parser
from exception import CustomException


class setup_project(convert_2_file):
    def __init__(self):
        
        self.setup_folder()
        self.setup_log()
        
        kwargs = vars(setup_parser().get_args)
        try:
            batch_run = kwargs['date'].strftime('%Y-%m-%d')
            
            logging.info(f"Start Run Batch Date: {batch_run}")
            super().__init__(kwargs=kwargs)
            
        except CustomException as errors:
            logging.error("Error Exception")
            while True:
                try:
                    msg_err = next(errors)
                    logging.error(msg_err)
                except StopIteration:
                    break
        finally:
            logging.info(f"Stop Batch Date {batch_run}\n")
            
        self.clear_folder()
        
if __name__ == "__main__":
    setup_project()
    