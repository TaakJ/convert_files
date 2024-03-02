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
import operator
import openpyxl

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
    insert_rows = []
    diff_rows = {}
    
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
        logging.info("Generate Excel files to Dataframe..")
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
        logging.info("Generate Text files to Dataframe..")
        key = {}
        rows = 0
        line_regex = iter(cls.clean_lines_text(full_path))
        
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
    def check_up_data(cls, diff_df, new_df):
        logging.info("Check Changed Data or Not Change in Data..")
        
        if len(diff_df.index) > len(new_df.index):
            cls.skip_rows = [idx for idx in list(diff_df.index) if idx not in list(new_df.index)]
        
        ## reset index data
        union_index = np.union1d(diff_df.index, new_df.index)
        
        ## target / tmp data
        diff_df = diff_df.reindex(index=union_index, columns=diff_df.columns).iloc[:,:-1]
        
        ## new data
        new_df = new_df.reindex(index=union_index, columns=new_df.columns).iloc[:,:-1]
        
        # compare data rows by rows
        diff_df['changed'] = pd.DataFrame(np.where(diff_df.ne(new_df), True, False), index=diff_df.index, columns=diff_df.columns)\
            .apply(lambda x: (x==True).sum(), axis=1)
        
        start_rows = 2
        for idx in union_index:
            if idx not in cls.skip_rows:
                
                changed_value = {}
                for diff, new in zip(diff_df.items(), new_df.items()):
                    if diff_df.loc[idx, 'changed'] != 14:
                        
                        if diff_df.loc[idx, 'changed'] <= 1: 
                            diff_df.at[idx, diff[0]] = diff[1].iloc[idx]
                            diff_df.loc[idx, 'recoreded'] = 'No_changed'
                            
                        else:
                            if (diff[1][idx] != new[1][idx]): 
                                changed_value.update({diff[0]: f"{diff[1][idx]} -> {new[1][idx]}"}) 
                            cls.diff_rows[start_rows + idx] = changed_value
                            
                            diff_df.at[idx, diff[0]] = new[1].iloc[idx]
                            diff_df.loc[idx, 'recoreded'] = 'Updated'
                            
                    else:
                        changed_value.update({diff[0]: f"{diff[1][idx]} -> {new[1][idx]}"})
                        cls.diff_rows[start_rows + idx] = changed_value
                        
                        diff_df.at[idx, diff[0]] = new[1].iloc[idx]
                        diff_df.loc[idx, 'recoreded'] = 'Inserted'
            else:
                diff_df.loc[idx, 'recoreded'] = 'Removed'
        
        cls.skip_rows = [start_rows + row for row in cls.skip_rows]
        
        diff_df = diff_df.drop(['changed'], axis=1)
        diff_df = diff_df.to_dict('index')
        output = {start_rows + row: diff_df[row] for row in diff_df}  
        
        return output
    
    @classmethod
    def append_target_data(cls, target_df, tmp_df):
        ''
        
        # target_df = pd.read_excel(target_name)
        # start_rows = 2
        # output = {}

        # date = tmp_df['CreateDate'].unique()
        
        # ## select row not use for compare
        # mark_df = target_df[~target_df['CreateDate'].isin(date)].to_dict('index')
        # max_rows = max(mark_df, default=0)
        
        # ## select row use for compare / mark data for compare with tmp
        # select_df = target_df[target_df['CreateDate'].isin(date)].reset_index(drop=True)
        # compare_data = cls.check_up_data(select_df, tmp_df)
        # select_date = select_df.to_dict('index')
        
        # ## compare target change / not change
        # for key, value in compare_data.items():
        #     if key not in cls.skip_rows:
        #         try:
        #             if value != select_date[key]:
        #                 select_date.pop(key)
        #             select_date[key] = value
        #         except KeyError:
        #             select_date[key] = value
        #     else:
        #         if value == select_date[key]:
        #             select_date.pop(key)
                    
        # for value in select_date.values():
        #     max_rows += 1
        #     mark_df[max_rows] = value
        #     mark_df[max_rows]['inserted'] =  True
        
        # ## set ordered rows 
        # ordered = sorted([mark_df[value] for value in mark_df], key=operator.itemgetter('CreateDate'))
        # sorted_rows = iter(ordered)
        # while True:
        #     try:
        #         value = next(sorted_rows)
        #         output.update({start_rows: value})
                
        #         if value.get('inserted'):
        #             cls.insert_rows.append(start_rows)
        #             value.pop('inserted')
                    
        #     except StopIteration:
        #         break
        #     start_rows += 1
            
    # else:
    #     tmp_df = tmp_df.to_dict('index')
    #     output = {start_rows + key: value for key,value in tmp_df.items()}
    
        return 'output'