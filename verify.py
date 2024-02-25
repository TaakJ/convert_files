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
import numpy as np
from collections import Counter

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
    def clean_lines_text(full_path):
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
    def generate_excel_data(cls, full_path):
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
    def generate_text_data(cls, full_path):
        key = {}
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
                        
                    if sheets not in key:
                        key[sheets] = [clean_data]
                    else:
                        key[sheets].append(clean_data)
                rows += 1
                
            except StopIteration:
                break  
        return key
    
    
    @classmethod
    def update_data(cls, old_df, new_df):
        
        if len(old_df.index) > len(new_df.index):
            cls.skip_rows = [idx for idx in list(old_df.index) if idx not in list(new_df.index)]
        
        union_index = np.union1d(old_df.index, new_df.index)
        ## set old record
        old_df = old_df.reindex(index=union_index, columns=old_df.columns)
        
        ## set new record
        new_df = new_df.reindex(index=union_index, columns=new_df.columns)
        
        ## set column change / skip in new_df 
        new_df['change'] = pd.DataFrame(np.where(new_df.ne(old_df), True, False), index=new_df.index, columns=new_df.columns)\
            .apply(lambda x: (x==True).sum(), axis=1)
        new_df['skip'] = new_df.apply(lambda x: x.isna()).all(axis=1)
        
        ## set column change / skip in old_df 
        old_df['change'] = new_df['change']
        old_df['skip'] = old_df.apply(lambda x: x.isna()).all(axis=1)
        
        for idx in union_index:
            if idx not in cls.skip_rows:
                for old, new in zip(old_df.items(), new_df.items()):
                    if old_df.loc[idx, 'skip'] == new_df.loc[idx, 'skip']:
                        if new_df.loc[idx, 'change'] <= 1:
                            ## not change rows
                            old_df.at[idx, old[0]] = old[1].iloc[idx]
                        else:
                            ## update rows
                            old_df.at[idx, old[0]] = new[1].iloc[idx]
                    else:
                        ## insert rows
                        old_df.at[idx, old[0]] = new[1].iloc[idx]
            else:
                ## delete rows
                continue
            
        old_df = old_df.loc[old_df['change'] > 1].drop(['change', 'skip'], axis=1)
        to_write = old_df.to_dict('index')
        return to_write
    
    @classmethod
    def get_data_target(cls, target_name, tmp_df):
        
        target_df = pd.read_excel(target_name)
        
        if not target_df.empty:
            
            ## select data row for daily
            date = tmp_df['CreateDate'].unique()
            mask = target_df['CreateDate'].isin(date)
            target_df = target_df[mask].reset_index(drop=True)
            
            output = cls.update_data(target_df, tmp_df)
            to_write = target_df.to_dict('index')
            
            for key, value in output.items():
                if key not in to_write:
                    i = max(to_write) + 1
                    to_write[i] = value
                else:
                    continue
        else:
            to_write = tmp_df.to_dict('index')
            
        return to_write
            