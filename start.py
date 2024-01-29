import re
import os
import glob
import pandas as pd
from pathlib import Path
from os.path import join
from io import StringIO
import datetime
import chardet
import openpyxl 
from openpyxl.utils.dataframe import dataframe_to_rows


CURRENT_DIR = os.getcwd()

class CustomException(Exception):
    def __init__(self, msg_err, **kwargs):
        # self.__dict__.update(kwargs)
        self.num = len(msg_err)
        
        self._msg_err = msg_err
        self._msg_err = self._generate_message(msg_err)
        
    def __next__(self):
        return next(self._msg_err)
    
    def _generate_message(self, msg_err):
        for i in range(self.num):
            if msg_err[i]['status'] == 'Success':
                fg, bg = '\x1b[6;37;42m', '\x1b[0m'
            else:
                fg, bg = '\x1b[6;37;41m', '\x1b[0m'
            message = f"\033[1mTemplate:\033[0;0m '{msg_err[i]['source']}'\t\033[1mFull_path:\033[0;0m '{msg_err[i]['full_path']}'\t\033[1mStatus:\033[0;0m {fg}'{msg_err[i]['status']}'{bg}"
            yield message
            
class FOLDER:
    RAW = join(CURRENT_DIR, "RAW/")
    EXCEL = join(CURRENT_DIR, "EXCEL/")
    CSV  = join(CURRENT_DIR, "CSV/")

class convert_file_to_csv:
    def __init__(self, method_args):
        
        self.template = ['ADM.txt', 'BOS.xlsx', 'CUM.xls', 'DocImage.txt', 'ICAS-NCR.xlsx', 'IIC.xlsx', 'LDS-P_UserDetail.txt', 'Lead-Management.xlsx', 'MOC.xlsx']
        self.run = method_args.run
        self.output = method_args.output
        
        self.get_list_files()
        self.get_data_files()
        
    @property
    def fn_log(self):
        return self.__log
    
    @fn_log.setter
    def fn_log(self, log):
        self.__log = list({dictionary['source']: dictionary for dictionary in log}.values())
    
    def check_success_files(call_func):
        def fn_success_files(self):
            
            success_file = []
            for _dict in call_func(self):
                filename = _dict['full_path']
                
                full_path = FOLDER.RAW + filename
                if glob.glob(full_path, recursive=True):
                    status = 'Success'
                    success_file.append(status)
                else:
                    status = 'Missing'
                    success_file.append(status)
                    
                _dict.update({'full_path': full_path, 'status': status})
            
            ## check success file
            if success_file.__contains__('Missing'):
                raise CustomException(self.fn_log)
            
            return self.fn_log
        return fn_success_files
    
    @check_success_files
    def get_list_files(self):
        
        fn_log = []
        if self.run == 0:
            for file in self.template:
                source = Path(file).stem
                fn_log.append({'source': source, 'full_path': file})
        
        self.fn_log = fn_log
        return self.fn_log
    
    def write_to_file(call_func):
        date = datetime.datetime.now().strftime('%Y%m%d')
        excel = f"{FOLDER.EXCEL}excel_{date}.xlsx" 
        wb = openpyxl.Workbook()
        wb.active
        
        def fn_write(self):
            for _dict in call_func(self):
                for name, data in _dict['data'].items():  
                    df = pd.DataFrame(data)
                    try:
                        if self.output == 1:
                            sheet = wb.create_sheet(name)
                            # sheet.title = sheet
                            rows = dataframe_to_rows(df, index=False , header=True)
                            for rdx, row in enumerate(rows, 1):
                                for cdx, val in enumerate(row, 1):
                                    wf = sheet.cell(row=rdx, column=cdx)
                                    wf.value = val
                            wb.save(excel)
                            
                        elif self.output == 2:
                            df = pd.DataFrame(data)
                            csv_name = f"{FOLDER.CSV}{name}{date}.csv"
                            df.to_csv(csv_name, index=False, float_format='%g')
                            
                        else:
                            print("OK")
                            
                    except Exception as err:
                        _dict.update({'errors': str(err)})
                        
                _dict.update({'write': 'compalted'})
        
        return fn_write

    @write_to_file
    def get_data_files(self):
        
        for _dict in self.fn_log:
            full_path = _dict['full_path']
            types = Path(_dict['full_path']).suffix
            
            try:
                if ['.xlsx', '.xls'].__contains__(types):
                    data_list = self.generate_excel_dataframe(full_path)
                else:
                    data_list = self.generate_text_dataframe(full_path)
                    
                _dict.update({'data': data_list})
                
            except Exception as err:
                _dict.update({'errors': str(err)})
        
        if 'errors' in self.fn_log[0]:
            raise CustomException(self.fn_log)
            
        return self.fn_log
    
    @staticmethod
    def generate_excel_dataframe(full_path):
        
        data_list = {}
        sheet_list =  [sheet for sheet in pd.ExcelFile(full_path).sheet_names if sheet != 'StyleSheet']
        
        for name in sheet_list:
            df = pd.read_excel(full_path, sheet_name=name, header=0)
            df.columns = [value  if 'Unnamed' in col else col for col, value in df.iloc[0].items()]
            
            if set(df.columns.values) == set(df.iloc[0].values):
                df = df.drop(index=0, axis=0).reset_index(drop=True)
            df_new = df.to_dict('records')
            
            data_list[name] = df_new    
        return data_list
        
    @staticmethod
    def generate_text_dataframe(full_path):
        
        data_list = {}
        ## get_decoded_data
        files = open(full_path, 'rb')
        encoded = chardet.detect(files.read())['encoding']
        files.seek(0)
        decoded_data = files.read().decode(encoded)
        
        name =  str(Path(full_path).stem).upper()
        
        clean_lines_column = []
        clean_lines_value = []
        for line_num, line in enumerate(StringIO(decoded_data)):
            regex = re.compile(r'\w+.*')
            line_list = regex.findall(line)
            
            if line_list != []:
                gen_regex = re.sub(r'\W\s+','||',"".join(line_list).strip()).split('||')
                
                ## LDS-P_USERDETAIL ##
                if name == 'LDS-P_USERDETAIL':
                    if line_num == 0:
                        for col in gen_regex:
                            clean_lines_column = "".join(col).split(' ')
                    else:
                        nested_lines = [] 
                        for n, val in enumerate(gen_regex):
                            if n == 0:
                                val = "".join(val).split(' ')
                                nested_lines.extend(val)
                            else:
                                nested_lines.append(val)
                        clean_lines_value.append(nested_lines)
                
                ## DOCIMAGE ##     
                elif name == 'DOCIMAGE':
                    if line_num == 5:
                        nested_lines = []
                        for n_col, col in enumerate(gen_regex):
                            if n_col == 4:
                                col = "".join(col).split(' ')
                                clean_lines_column.extend(col)
                            else:
                                clean_lines_column.append(col)
                    elif line_num > 5:
                        nested_lines = []
                        for n, val in enumerate(gen_regex):
                            if n == 3:
                                val = "".join(val).split(' ')
                                nested_lines.extend(val)
                            else:
                                nested_lines.append(val)
                        clean_lines_value.append(nested_lines)
                        
                ## ADM ##     
                elif name == 'ADM':
                    nested_lines = []
                    for val in gen_regex:
                        nested_lines.append(val)
                    clean_lines_value.append(nested_lines)
        
        df = pd.DataFrame(clean_lines_value)
        if clean_lines_column != []:
            df.columns = clean_lines_column
            
        df_new = df.to_dict('records')
        data_list[name] = df_new 
        return data_list
                    