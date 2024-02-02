import argparse
from start import convert_file_to_csv, CustomException
from verify import FOLDER, verify_files

        
def setup_project():
    
    FOLDER.setup_log()
    FOLDER.setup_folder()
    
    # parser = argparse.ArgumentParser()
    # parser.add_argument("-r","--run",
    #                     required=False, 
    #                     type=int,
    #                     default=0,
    #                     choices=[0,1],
    #                     help = (' 0 = manual '
    #                             ' 1 = schedule ')
    #                     )
    # parser.add_argument("-o","--output",
    #                     required=False, 
    #                     choices=[1,2],
    #                     type=int,
    #                     default=1,
    #                     help = (' 0 = Excel file , '
    #                             ' 1 = CSV file',
    #                             ' 2 = text file ')
    #                     )
    # method_args = parser.parse_args()
    
    # try:
    #     logging.info("Start Project")
    #     convert_file_to_csv(method_args)
        
    # except CustomException as err:  
    #     logging.error("Error Exception")     
    #     err_list = iter(err)
    #     while True:
    #         try:
    #             msg_err = next(err_list)
    #             logging.error(msg_err)
    #         except StopIteration:
    #             break
    #     logging.error(f"File Found Count {err.n} Status: Success")
    # finally:
    #     logging.info("End Project")
    
    # full_path = FOLDER.RAW + 'BOS.xlsx' # MOC.xlsx / CUM.xls / BOS.xlsx
    # b = verify_files.generate_excel_dataframe(full_path)
    FOLDER.clear_folder()

if __name__ == "__main__":
    setup_project()
    