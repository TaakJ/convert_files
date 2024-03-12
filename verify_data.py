import re
import xlrd
import logging.config
import chardet
from io import StringIO
from pathlib import Path
import pandas as pd
import numpy as np

class validate_files:
    def __init__(self):
        self.upsert_rows = {}
        self.skip_rows = []

    def clean_lines_excel(func):
        def wrapper_clean_lines(*args, **kwargs):
            clean_lines = iter(func(*args, **kwargs))

            clean_data = {}
            while True:
                try:
                    for sheets, data in next(clean_lines).items():
                        if not all(dup == data[0] for dup in data) and not data.__contains__("Centralized User Management : User List."):
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
        sheet_list = [sheet for sheet in workbook.sheet_names() if sheet != "StyleSheet"]

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
                        if sheets == "LDS-P_USERDETAIL":
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
                        elif sheets == "DOCIMAGE":
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
                        elif sheets == "ADM":
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

    def validation_data(self, diff_df, new_df):

        logging.info("Verify Changed information..")

        if len(diff_df.index) > len(new_df.index):
            self.skip_rows = [idx for idx in list(diff_df.index) if idx not in list(new_df.index)]
        
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
            if idx not in self.skip_rows:
                
                _value = {}
                for diff, new in zip(diff_df.items(), new_df.items()):
                    if diff_df.loc[idx, 'changed'] != 14:
                        if diff_df.loc[idx, 'changed'] <= 1:
                            ## No_changed rows
                            diff_df.at[idx, diff[0]] = diff[1].iloc[idx]
                            diff_df.loc[idx, 'recoreded'] = "No_changed"
                        else:
                            if diff[1][idx] != new[1][idx]:
                                _value.update({diff[0]: f"{diff[1][idx]} -> {new[1][idx]}"})
                            self.upsert_rows[start_rows + idx] = _value
                            ## Updated rows
                            diff_df.at[idx, diff[0]] = new[1].iloc[idx]
                            diff_df.loc[idx, 'recoreded'] = "Updated"
                    else:
                        _value.update({diff[0]: f"{new[1][idx]}"})
                        self.upsert_rows[start_rows + idx] = _value
                        ## Inserted rows
                        diff_df.at[idx, diff[0]] = new[1].iloc[idx]
                        diff_df.loc[idx, 'recoreded'] = "Inserted"
            else:
                ## Removed rows
                diff_df.loc[idx, 'recoreded'] = "Removed"
                
        self.skip_rows = [idx + start_rows for idx in self.skip_rows]
        diff_df = diff_df.drop(['changed'], axis=1)
        diff_df.index += start_rows 
        new_data = diff_df.to_dict('index')
        
        return new_data

    def append_target_data(self, select_date, target_df, tmp_df):

        logging.info("Append Target Data..")

        ## unique_date
        unique_date = target_df[target_df['CreateDate'].isin(select_date)].reset_index(drop=True)
        ## other_date
        other_date = target_df[~target_df['CreateDate'].isin(select_date)].iloc[:, :-1].to_dict('index')
        max_rows = max(other_date, default=0)
        ## compare data target / tmp
        compare_data = self.validation_data(unique_date, tmp_df)
        ## add value to other_date
        other_date = other_date | {max_rows + key:  {**values, **{'upsert_rows': key}} \
            if key in self.upsert_rows or key in self.skip_rows \
                else values for key, values in compare_data.items()}
        
        ## sorted date value
        idx = 0
        start_row = 2
        new_data = {start_row + idx :values for idx, values  in enumerate(sorted(other_date.values(), key=lambda x: x['CreateDate']))}
        ## update upsert_rows / skip_rows
        for key, values in new_data.items():
            if values.get('upsert_rows'):
                if values['upsert_rows'] in self.upsert_rows:
                    self.upsert_rows[key] = self.upsert_rows.pop(values['upsert_rows'])
                elif values['upsert_rows'] in self.skip_rows:
                    self.skip_rows[idx] = key
                    idx += 1
                values.pop('upsert_rows')
        
        return new_data