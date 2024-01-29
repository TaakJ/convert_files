import argparse
import os
import yaml
import shutil
import logging.config 
from start import convert_file_to_csv, CustomException, FOLDER, LOGGER_CONFIG

def setup_log():
    _yaml = None
    if os.path.exists(LOGGER_CONFIG):
        with open(LOGGER_CONFIG, 'r') as f:
            _yaml = yaml.safe_load(f)
            logging.config.dictConfig(_yaml)
    else:
        raise Exception(f"Yaml file file_path: '{LOGGER_CONFIG}' doesn't exist")

def setup_folder():
    _folders = [value for name, value in vars(FOLDER).items() if not name.startswith('_')]
    for folder in _folders:
        os.makedirs(folder, exist_ok=True)
        
def clear_folder():
    _folders = [value for name, value in vars(FOLDER).items() if not name.startswith('_') and not value.endswith('RAW/')]
    for folder in _folders:
        shutil.rmtree(folder)
        
def setup():
    setup_folder()
    setup_log()
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
    try:
        a = convert_file_to_csv(method_args)
        logging.info("completed")
    except CustomException as err:
        [logging.error(f"{err.__next__()}") for i in range(err.num)]
        
    clear_folder()

if __name__ == "__main__":
    setup()
    