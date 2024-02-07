import argparse
import logging
from start import convert_file_to_csv, CustomException
from verify import FOLDER

def setup_project():

    FOLDER.backup_folder()
    FOLDER.clear_folder()
    FOLDER.setup_folder()
    FOLDER.setup_log()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-s","-RAW",
                        required=False, 
                        type=str
                        )
    parser.add_argument("-x","--EXPORT",
                        required=False, 
                        type=str
                        )
    parser.add_argument("-o","--output",
                        required=False, 
                        choices=[1,2],
                        type=int,
                        default=1,
                        help = (' 0 = Excel file , '
                                ' 1 = CSV file',
                                ' 2 = text file ')
                        )
    method_args = parser.parse_args()
    
    try:
        logging.info("Start Project")
        convert_file_to_csv(method_args)
        
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
    setup_project()
    