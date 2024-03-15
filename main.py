import logging
from run_batch import convert_2_file
from setup import setup_parser, setup_folder, setup_log, clear_tmp
from exception import CustomException

class setup_project(convert_2_file):
    def __init__(self):
        setup_folder()
        setup_log()
        
        try:
            params = setup_parser().parsed_params
            logging.info(f"Start Run Batch Date: {params.batch_date}")
            super().__init__(**vars(params))
            
        except CustomException as errors:
            logging.error("Error Exception")
            while True:
                try:
                    msg_err = next(errors)
                    logging.error(msg_err)
                except StopIteration:
                    break
        logging.info(f"Stop Batch Date\n")
        
        if not params.tmp: 
            clear_tmp()
        
if __name__ == "__main__":
    setup_project()
    