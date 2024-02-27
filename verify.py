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
    def update_data(cls, target_df, new_df):
        
        if len(target_df.index) > len(new_df.index):
            cls.skip_rows = [idx for idx in list(target_df.index) if idx not in list(new_df.index)]
        
        union_index = np.union1d(target_df.index, new_df.index)
        ## set old record
        target_df = target_df.reindex(index=union_index, columns=target_df.columns)
        
        ## set new record
        new_df = new_df.reindex(index=union_index, columns=new_df.columns)
        
        ## set column change / skip in new_df 
        new_df['change'] = pd.DataFrame(np.where(new_df.ne(target_df), True, False), index=new_df.index, columns=new_df.columns)\
            .apply(lambda x: (x==True).sum(), axis=1)
        new_df['skip'] = new_df.apply(lambda x: x.isna()).all(axis=1)
        
        ## set column change / skip in old_df 
        target_df['change'] = new_df['change']
        target_df['skip'] = target_df.apply(lambda x: x.isna()).all(axis=1)
        
        for idx in union_index:
            if idx not in cls.skip_rows:
                for target, new in zip(target_df.items(), new_df.items()):
                    if target_df.loc[idx, 'skip'] == new_df.loc[idx, 'skip']:
                        if new_df.loc[idx, 'change'] <= 1:
                            ## not change rows
                            target_df.at[idx, target[0]] = target[1].iloc[idx]
                        else:
                            ## update rows
                            target_df.at[idx, target[0]] = new[1].iloc[idx]
                    else:
                        ## insert rows
                        target_df.at[idx, target[0]] = new[1].iloc[idx]
            else:
                ## delete rows
                continue
            
        target_df = target_df.loc[target_df['change'] > 1].drop(['change', 'skip'], axis=1)
        output = target_df.to_dict('index')
        return output
    
    @classmethod
    def get_data_target(cls, target_name, tmp_df):
        
        target_df = pd.read_excel(target_name)
        
        if not target_df.empty:
            date = tmp_df['CreateDate'].unique()
            
            ## select row not use for compare
            output = target_df[~target_df['CreateDate'].isin(date)]
            get_rows = len(output.index)
            
            ## select row use for compare / mark data for compare with tmp
            mask_df = target_df[target_df['CreateDate'].isin(date)].reset_index(drop=True)
            mask_diff = mask_df.to_dict('index')
            compare_data = cls.update_data(mask_df, tmp_df)
            
            for key, value in compare_data.items():
                if key not in cls.skip_rows:
                    try:
                        if value != mask_diff[key]:
                            mask_diff.pop(key)
                        mask_diff[key] = value
                    except KeyError:
                        mask_diff[key] = value
                else:
                    if value == mask_diff[key]:
                        mask_diff.pop(key)
            
            ## update data for wirte to target
            output = output.to_dict('index')
            if output != {}:
                max_rows = get_rows - 1
                for key, value in mask_diff.items():
                    max_rows += 1
                    output[max_rows] = value
            else:
                output = mask_diff
                
            start_rows = 2
            for i, lines in enumerate(cls.skip_rows):
                skip_rows = (start_rows + lines) + get_rows
                cls.skip_rows[i] = skip_rows
        else:
            output = tmp_df.to_dict('index')
            
        return output