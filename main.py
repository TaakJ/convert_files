import argparse
import logging
from start import convert_2_file, CustomException
from datetime import datetime


# def setup_project():
#     parser = argparse.ArgumentParser()
#     parser.add_argument("-s","-RAW",
#                         required=False, 
#                         type=str
#                         )
#     parser.add_argument("-x","--EXPORT",
#                         required=False, 
#                         type=str
#                         )
#     parser.add_argument("-o","--output",
#                         required=False, 
#                         choices=[1,2],
#                         type=int,
#                         default=1,
#                         help = (' 0 = Excel file , '
#                                 ' 1 = CSV file',
#                                 ' 2 = text file ')
#                         )
#     method_args = parser.parse_args()

class start_project(convert_2_file):
    def __init__(self):
        
        self.backup_folder()
        self.clear_folder()
        self.setup_folder()
        self.setup_log()
        
        try:
            logging.info("Start Project")
            date = datetime.now()
            super().__init__(date=date)
            
        except CustomException as err:  
            logging.error("Error Exception")     
            err_list = iter(err)
            while True:
                try:
                    msg_err = next(err_list)
                    logging.error(msg_err)
                except StopIteration:
                    break
            logging.error(f"File Found Count {err.n} Status: Success")
        
        finally:
            logging.info("End Project")
            
if __name__ == "__main__":
    start_project()
    