import argparse
import re
from start import convert_file_to_csv, CustomException

def setup():
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-r","--run",
                        required=False, 
                        type=int,
                        default=0,
                        choices=[0,1],
                        help = (' 0 = manual '
                                ' 1 = schedule ')
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
    
    ## call functions
    try:
        convert_file_to_csv(method_args)
    except CustomException as err:
        [print(f"Run Error: {err.__next__()}") for i in range(err.num)]
    
if __name__ == "__main__":
    setup()