import re
import os
from os.path import join
import xlrd
import shutil
import yaml
import logging.config
import chardet
from datetime import datetime
from io import StringIO
from pathlib import Path
import pandas as pd
from itertools import zip_longest

CURRENT_DIR = os.getcwd()
LOGGER_CONFIG = join(CURRENT_DIR, 'logging_config.yaml') 

class FOLDER:
    RAW         = join(CURRENT_DIR, "raw/")
    EXPORT      = join(CURRENT_DIR, "export/")
    LOG         = join(CURRENT_DIR, "tmp/log/")
    TEMPLATE    = ['ADM.txt', 'BOS.xlsx', 'CUM.xls', 'DocImage.txt', 'ICAS-NCR.xlsx', 'IIC.xlsx', 'LDS-P_UserDetail.txt', 'Lead-Management.xlsx', 'MOC.xlsx']
    
    @staticmethod
    def setup_log():
        _yaml = None
        if os.path.exists(LOGGER_CONFIG):
            with open(LOGGER_CONFIG, 'r') as logger:
                _yaml = yaml.safe_load(logger)
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
        _folders = [value for name, value in vars(FOLDER).items() if isinstance(value, str) and not name.startswith('_') and not value.endswith(('raw/','export/'))]
        for folder in _folders:
            shutil.rmtree(folder)
    
    @staticmethod
    def backup_folder():
        date = datetime.now().strftime('%d%m%Y')
        bk_path = join(FOLDER.EXPORT, f"BK_{date}")
        
        if not os.path.exists(bk_path):
            os.makedirs(bk_path)
        else:
            shutil.rmtree(bk_path)
            os.makedirs(bk_path)
        
        _folders = [value for name, value in vars(FOLDER).items() if isinstance(value, str) and not name.startswith('_') and value.endswith(('export/','log/'))]
        for folder in _folders:
            for files in os.listdir(folder):
                if files.endswith((".xlsx",'.log')):
                    shutil.copy2(join(folder, files), bk_path)
                    
                    
class validate_files(FOLDER):
    
    skip_rows = []
    
    @staticmethod
    def read_export_file_daily(target_name):
        
        list_data = []
        workbook = xlrd.open_workbook(target_name)
        sheet = workbook.sheet_by_index(0)
        rows = sheet.get_rows()
        
        for row in rows:
            if all([cell.ctype in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK) for cell in row]):
                break
            else:
                list_data.append(row)
                
        target_df = pd.DataFrame(list_data)
        target_df.columns = target_df.iloc[0].values
        target_df = target_df[1:]
        target_df = target_df.reset_index(drop=True)
        
        return target_df
    
    @classmethod
    def compare_target(cls, target_name, csv_df):
        
        target_df = cls.read_export_file_daily(target_name)
        print(target_df)
    
    @staticmethod
    def clean_lines_excel(full_path):
        workbook = xlrd.open_workbook(full_path)
        sheet_list = [sheet for sheet in workbook.sheet_names() if sheet != 'StyleSheet']
        
        key = {}
        for sheets in sheet_list:
            cells = workbook.sheet_by_name(sheets)
            for row in range(0, cells.nrows):
                key = {sheets: [cells.cell(row, col).value for col in range(cells.ncols)]}
                yield key
                
    @staticmethod
    def clean_lines_txt(full_path):
        sheets =  str(Path(full_path).stem).upper()
        files = open(full_path, 'rb')
        encoded = chardet.detect(files.read())['encoding']
        files.seek(0)
        decode_data = StringIO(files.read().decode(encoded))
        
        key = {}
        for line in decode_data:
            regex = re.compile(r'\w+.*')
            line_regex = regex.findall(line)
            
            if line_regex != []:
                key = {sheets: re.sub(r'\W\s+','||',"".join(line_regex).strip()).split('||')}
                yield key
    
    @classmethod
    def generate_excel_df(cls, full_path):
        key = {}
        clean_data = iter(cls.clean_lines_excel(full_path))
        
        while True:
            try:
                for sheets, data in next(clean_data).items():
                    if not all(dup == data[0] for dup in data) and not data.__contains__('Centralized User Management : User List.'):
                        if sheets not in key:
                            key[sheets] = [data]
                        else:
                            key[sheets].append(data)
            except StopIteration:
                break
        return key
                
    @classmethod
    def generate_txt_df(cls, full_path):
        key = {}
        line_regex = iter(cls.clean_lines_txt(full_path))
        
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
                        
                    if sheets not in key:
                        key[sheets] = [clean_data]
                    else:
                        key[sheets].append(clean_data)
                rows += 1
                
            except StopIteration:
                break  
        return key
    
    @classmethod
    def generate_tmp_df(cls, csv_df, new_df):
        
        if len(csv_df.index) > len(new_df.index):
            cls.skip_rows = [idx for idx in list(csv_df.index) if idx not in list(new_df.index)]
            
        df = pd.DataFrame()
        for csv_idx, new_idx in zip_longest(list(csv_df.index), list(new_df.index)):
            if csv_idx not in cls.skip_rows:
                i = 0
                cnt_change = []
                for (col, val), (_, val_diff) in zip_longest(csv_df.items(), new_df.items()):
                    if csv_idx == new_idx:
                        if val.iloc[new_idx] == val_diff.iloc[new_idx]:
                            ## not change record
                            df.at[new_idx, col] = val.iloc[new_idx]
                        else:
                            i += 1
                            cnt_change += [{new_idx:i}]
                            ## not change record / but change only column LastUpdatedDate
                            if len(cnt_change) == 1 and col == 'LastUpdatedDate':
                                df.at[new_idx, col] = val.iloc[new_idx]
                            else:
                                ## change record
                                df.at[new_idx, col] = val_diff.iloc[new_idx]
                            df.at[new_idx, 'change'] = len(cnt_change)
                                
                    elif csv_idx is None and new_df is not None:
                        ## insert record
                        df.at[new_idx, col] = val_diff.iloc[new_idx]
                        df.at[new_idx, 'change'] = 14
            else:
                df.loc[csv_idx] = 'skip_rows'
                df.at[csv_idx, 'change'] = 14
        
        df = df.loc[df['change'] > 1].drop(['change'], axis=1)
        to_update = {idx: rows for idx, rows in df.to_dict('index').items()}
        
        return to_update