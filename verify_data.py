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
import pandas as pd
import numpy as np
import operator

CURRENT_DIR = os.getcwd()
LOGGER_CONFIG = join(CURRENT_DIR, 'logging_config.yaml')

class FOLDER:
    RAW         = join(CURRENT_DIR, "raw/")
    EXPORT      = join(CURRENT_DIR, "export/")
    TMP         = join(CURRENT_DIR, "tmp/export/")
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

    # @staticmethod
    # def backup_folder():
    #     date = datetime.now().strftime('%d%m%Y')
    #     bk_path = join(FOLDER.EXPORT, f"BK_{date}")
    #     if not os.path.exists(bk_path):
    #         os.makedirs(bk_path)
    #     else:
    #         shutil.rmtree(bk_path)
    #         os.makedirs(bk_path)
    #     _folders = [value for name, value in vars(FOLDER).items() if isinstance(value, str) and not name.startswith('_') and value.endswith(('export/','log/'))]
    #     for folder in _folders:
    #         for files in os.listdir(folder):
    #             if files.endswith((".xlsx",'.log')):
    #                 shutil.copy2(join(folder, files), bk_path)


class validate_files(FOLDER):
    diff_rows = {}
    skip_rows = []
                
    def clean_lines_excel(func):
        def wrapper_clean_lines(*args, **kwargs):
            clean_lines = iter(func(*args, **kwargs))
            
            clean_data = {}
            while True:
                try:
                    for sheets, data in next(clean_lines).items():
                        if not all(dup == data[0] for dup in data) and not data.__contains__('Centralized User Management : User List.'):
                            if sheets not in clean_data:
                                clean_data[sheets] = [data]
                            else:
                                clean_data[sheets].append(data)
                except StopIteration:
                    break
                
            return clean_data
        return wrapper_clean_lines
                
    @clean_lines_excel
    def generate_excel_data(self, full_path):
        
        logging.info("Cleansing Data in Excel files to Dataframe..")
        
        workbook = xlrd.open_workbook(full_path);
        sheet_list = [sheet for sheet in workbook.sheet_names() if sheet != 'StyleSheet']
        
        clean_data = {}
        for sheets in sheet_list:
            cells = workbook.sheet_by_name(sheets)
            for row in range(0, cells.nrows):
                clean_data = {sheets: [cells.cell(row, col).value for col in range(cells.ncols)]}
                yield clean_data

    def clean_lines_text(func):
        def wrapper_clean_lines(*args, **kwargs):
            clean_lines = iter(func(*args, **kwargs))
            
            clean_data = {}
            rows = 0
            while True:
                try:
                    lines = []
                    for sheets, data in  next(clean_lines).items():
                        # LDS-P_USERDETAIL
                        if sheets == 'LDS-P_USERDETAIL':
                            if rows == 0:
                                lines = " ".join(data).split(' ') # column
                            else:
                                for idx, value in enumerate(data): # fix value
                                    if idx == 0:
                                        value = re.sub(r'\s+',',', value).split(',')
                                        lines.extend(value)
                                    else:
                                        lines.append(value)
                        ## DOCIMAGE
                        elif sheets == 'DOCIMAGE':
                            if rows == 1:
                                lines = " ".join(data).split(' ') # column
                            elif rows > 1:
                                for idx, value in enumerate(data): # fix value
                                    if idx == 3:
                                        value = re.sub(r'\s+',',', value).split(',')
                                        lines.extend(value)
                                    else:
                                        lines.append(value)
                        ## ADM
                        elif sheets == 'ADM':
                            lines = data
                                        
                        if sheets not in clean_data:
                            clean_data[sheets] = [lines]
                        else:
                            clean_data[sheets].append(lines)
                            
                    rows += 1
                except StopIteration:
                    break
                
            return clean_data
        return wrapper_clean_lines
        
    @clean_lines_text
    def generate_text_data(self, full_path):
        
        logging.info("Cleansing Data in Text files to Dataframe..")
        
        files = open(full_path, 'rb')
        encoded = chardet.detect(files.read())['encoding']
        files.seek(0)
        decode_data = StringIO(files.read().decode(encoded))
        sheets =  str(Path(full_path).stem).upper()
        
        clean_data = {}
        for line in decode_data:
            regex = re.compile(r'\w+.*')
            find_lines = regex.findall(line)
            if find_lines != []:
                clean_data = {sheets: re.sub(r'\W\s+','||',"".join(find_lines).strip()).split('||')}
                yield clean_data

    @classmethod
    def validation_data(cls, diff_df, new_df):
        
        logging.info('Verify Changed information..')
        
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
                            if diff[1][idx] != new[1][idx]:
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
    
    def append_target_data(self, target_df, tmp_df):
        
        print(target_df)
        print(tmp_df)
        
        logging.info("Append Target Data..")
        
        ## unique date
        date = tmp_df['CreateDate'].unique()
        
        ## compare data new data with target data (mask date)
        # validate_data = mark_date(date=date, use='use')
        # new_df = cls.validation_data(validate_data, tmp_df)
        
        ## unique date other (not mask date)
        # merge_df = mark_date(date=date)
        
        # diff_date = target_df[~target_df['CreateDate'].isin(date)].iloc[:,:-1]
        # diff_date = diff_date.to_dict('index')
        # max_rows = max(diff_date, default=0)
        
        # for key, value in new_df.items():
        #     if key in cls.diff_rows or key in cls.skip_rows:
        #         value['diff_rows'] = key
        #     max_rows += 1
        #     diff_date[max_rows] = value
        
        
        
        ## set ordered rows
        # output = {}
        # ordered = sorted([diff_date[value] for value in diff_date], key=operator.itemgetter('CreateDate'))
        # sorted_rows = iter(ordered)
        
        # print(sorted_rows)
        # idx = 0
        # start_rows = 2
        # while True:
        #     try:
        #         rows = next(sorted_rows)
        #         output.update({start_rows: rows})
        #         if rows.get('diff_rows'):
        #             ## diff rows
        #             if rows['diff_rows'] in cls.diff_rows:
        #                 cls.diff_rows[start_rows] = cls.diff_rows.pop(rows['diff_rows'])
        #             ## skip rows
        #             elif rows['diff_rows'] in cls.skip_rows:
        #                 cls.skip_rows[idx] = start_rows
        #                 idx += 1
        #             rows.pop('diff_rows')
        #     except StopIteration:
        #         break
        #     start_rows += 1
        return 'output'