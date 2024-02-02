import os
from os.path import join
import xlrd
import shutil
import yaml
import logging.config
import pandas as pd
import chardet
from io import StringIO

CURRENT_DIR = os.getcwd()
LOGGER_CONFIG = join(CURRENT_DIR, 'logging_config.yaml') 

class FOLDER(object):
    RAW         = join(CURRENT_DIR, "raw/")
    CSV         = join(CURRENT_DIR, "tmp/csv/")
    EXCEL       = join(CURRENT_DIR, "tmp/excel/")
    LOG       = join(CURRENT_DIR, "tmp/log/")
    
    @staticmethod
    def setup_log():
        _yaml = None
        if os.path.exists(LOGGER_CONFIG):
            with open(LOGGER_CONFIG, 'r') as f:
                _yaml = yaml.safe_load(f)
                logging.config.dictConfig(_yaml)
        else:
            raise Exception(f"Yaml file file_path: '{LOGGER_CONFIG}' doesn't exist")
        
    @staticmethod
    def setup_folder():
        _folders = [value for name, value in vars(FOLDER).items() if isinstance(value, str) and not name.startswith('_')]
        for folder in _folders:
            os.makedirs(folder, exist_ok=True)
            
    @staticmethod
    def clear_folder():
        _folders = [value for name, value in vars(FOLDER).items() if isinstance(value, str) and not name.startswith('_') and not value.endswith(('raw/','log/'))]
        for folder in _folders:
            shutil.rmtree(folder)
            
            
class verify_files(object):
    
    @property
    def fn_decoded(self):
        return self.__decoded
    
    @fn_decoded.setter 
    def fn_decoded(self, full_path):
        files = open(full_path, 'rb')
        encoded = chardet.detect(files.read())['encoding']
        files.seek(0)
        self.__decoded = StringIO(files.read().decode(encoded))
        
    @staticmethod
    def clean_lines_txt(full_path):
        
        _dict = {}
    
    @staticmethod
    def clean_lines_excel(full_path):
        
        workbook = xlrd.open_workbook(full_path)
        sheet_list = [sheet for sheet in workbook.sheet_names() if sheet != 'StyleSheet']
        
        _dict = {}
        for sheets in sheet_list:
            cells = workbook.sheet_by_name(sheets)
            for row in range(0, cells.nrows):
                _dict = {sheets: [cells.cell(row, col).value for col in range(cells.ncols)]}
                yield _dict
                
            logging.info(f"Read Sheetname: '{sheets}' Status: 'Succees'")
                
    @classmethod
    def generate_excel_dataframe(cls, full_path):
        
        _dict = {}
        fn_read = iter(cls.clean_lines_excel(full_path))
        while True:
            try:
                for sheets, data in  next(fn_read).items():
                    ## remove all empty value in list / for CUM.xls
                    if not all(dup == data[0] for dup in data) and not data.__contains__("Centralized User Management : User List."):
                        if sheets not in _dict:
                            _dict[sheets] = [data]
                        else:
                            _dict[sheets].append(data)
            except StopIteration:
                break
            
        logging.info(_dict)
