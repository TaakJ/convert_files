from setup import setup_parser, setup_folder, setup_log, clear_tmp
from exception import CustomException
from method import run_batch
import logging
class setup_project:
    setup_folder()
    setup_log()
    
    try:
        params = setup_parser().parsed_params
        logging.info(f"Start Run Batch Date: {params.batch_date}")
        run_batch(**vars(params))
        
    except CustomException as errors:
        logging.error("Error Exception")
        while True:
            try:
                msg_err = next(errors)
                logging.error(msg_err)
            except StopIteration:
                break
    logging.info(f"Stop Batch Date: {params.batch_date}\n")
    
    if not params.tmp: 
        clear_tmp()
        
if __name__ == "__main__":
    setup_project()
    