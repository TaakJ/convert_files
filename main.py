import logging
from run_batch import convert_2_file
from setup import setup_parser, setup_folder, setup_log, clear_folder
# from setup.setup_path import setup_log, setup_folder

from exception import CustomException


class setup_project(convert_2_file):
    def __init__(self):
        
        setup_folder()
        setup_log()
        
        # kwargs = vars(setup_parser().get_args)
        # try:
        #     batch_run = kwargs['date'].strftime('%Y-%m-%d')
            
        #     logging.info(f"Start Run Batch Date: {batch_run}")
        #     super().__init__(kwargs=kwargs)
            
        # except CustomException as errors:
        #     logging.error("Error Exception")
        #     while True:
        #         try:
        #             msg_err = next(errors)
        #             logging.error(msg_err)
        #         except StopIteration:
        #             break
        # finally:
        #     logging.info(f"Stop Batch Date {batch_run}\n")
            
        clear_folder()
        
if __name__ == "__main__":
    setup_project()
    