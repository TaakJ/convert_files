import re
import os
from os.path import join
import xlrd
import shutil
import yaml
import logging.config
import chardet
from io import StringIO
from pathlib import Path

CURRENT_DIR = os.getcwd()
LOGGER_CONFIG = join(CURRENT_DIR, 'logging_config.yaml') 

class FOLDER(object):
    RAW         = join(CURRENT_DIR, "raw/")
    TEMPLATE    = join(CURRENT_DIR, "template/")
    EXPORT      = join(CURRENT_DIR, "tmp/export")
    LOG         = join(CURRENT_DIR, "tmp/log/")
    
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
        _folders = [value for name, value in vars(FOLDER).items() if isinstance(value, str) and not name.startswith('_') and not value.endswith(('raw/','log/', 'template/'))]
        for folder in _folders:
            shutil.rmtree(folder)
            
            
class verify_files(object):
    
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
                
    @classmethod
    def generate_excel_dataframe(cls, full_path):
        
        _dict = {}
        clean_data = iter(cls.clean_lines_excel(full_path))
        while True:
            try:
                for sheets, data in next(clean_data).items():
                    if not all(dup == data[0] for dup in data) and not data.__contains__('Centralized User Management : User List.'):
                        if sheets not in _dict:
                            _dict[sheets] = [data]
                        else:
                            _dict[sheets].append(data)
                            
            except StopIteration:
                break
            
        return _dict
    
    @staticmethod
    def clean_lines_text(full_path):
        
        sheets =  str(Path(full_path).stem).upper()
        files = open(full_path, 'rb')
        encoded = chardet.detect(files.read())['encoding']
        files.seek(0)
        decode_data = StringIO(files.read().decode(encoded))
        
        _dict = {}
        for line in decode_data:
            regex = re.compile(r'\w+.*')
            line_regex = regex.findall(line)
            
            if line_regex != []:
                _dict = {sheets: re.sub(r'\W\s+','||',"".join(line_regex).strip()).split('||')}
                yield _dict
                
    @classmethod
    def generate_text_dataframe(cls, full_path):
        
        _dict = {}
        line_regex = iter(cls.clean_lines_text(full_path))
        
        rows = 0
        while True:
            try:
                clean_data = []
                for sheets, data in  next(line_regex).items():
                    # LDS-P_USERDETAIL ##
                    if sheets == 'LDS-P_USERDETAIL':
                        if rows == 0:
                            clean_data = " ".join(data).split(' ') # column
                        else:
                            for idx, value in enumerate(data): # fix value
                                if idx == 0:
                                    value = re.sub(r'\s+',',', value).split(',')
                                    clean_data.extend(value)
                                else:
                                    clean_data.append(value)
                                    
                    ## DOCIMAGE ##  
                    elif sheets == 'DOCIMAGE':
                        if rows == 1:
                            clean_data = " ".join(data).split(' ') # column
                        elif rows > 1:
                            for idx, value in enumerate(data): # fix value
                                if idx == 3:
                                    value = re.sub(r'\s+',',', value).split(',')
                                    clean_data.extend(value)
                                else:
                                    clean_data.append(value)
                    
                    ## ADM ##     
                    elif sheets == 'ADM':
                        clean_data = data
                    
                    if sheets not in _dict:
                        _dict[sheets] = [clean_data]
                    else:
                        _dict[sheets].append(clean_data)
                    _dict[sheets] = list(filter(lambda data: data != [], _dict[sheets]))
                
                rows += 1
                
            except StopIteration:
                break
        
        return _dict
    
    @staticmethod
    def read_template():
        data = []
        full_path = FOLDER.TEMPLATE + 'Application Data Requirements.xlsx'
        workbook = xlrd.open_workbook(full_path)
        ## sheet: Field Name
        sheet = workbook.sheet_by_index(0)
        rows = sheet.get_rows()
        next(rows)
        for row in rows:
            if all([cell.ctype in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK) for cell in row]):
                break
            else:
                data.append([cell.value for cell in row])
                
        if len(data) >= 1:
            print(data)
        

