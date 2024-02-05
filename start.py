import glob
import datetime
import openpyxl
import logging
import pandas as pd
from pathlib import Path
from openpyxl.utils.dataframe import dataframe_to_rows
from verify import FOLDER, verify_files

class CustomException(Exception):
    def __init__(self, err_list):
        self.n = 0
        self._msg_err = self._generate_meg_err(err_list)
        
    def __iter__(self):
        return self
    
    def __next__(self):
        return next(self._msg_err)
    
    def _generate_meg_err(self, err_list):
        for i in  range(len(err_list)):
            msg_err = f"Filename: '{err_list[i]['full_path']}' Status: '{err_list[i]['status']}' Error: '{err_list[i].get('errors')}'"
            if err_list[i]['status'] == 'Success':
                self.n += 1
            yield msg_err
            

class convert_file_to_csv:
    def __init__(self, method_args):
        self.template = ['ADM.txt', 'BOS.xlsx', 'CUM.xls', 'DocImage.txt', 'ICAS-NCR.xlsx', 'IIC.xlsx', 'LDS-P_UserDetail.txt', 'Lead-Management.xlsx', 'MOC.xlsx']
        self.run = method_args.run
        self.output = method_args.output
        
        self.get_list_files()
        self.get_data_files()
        # self.write_to_file()
        
    @property
    def fn_log(self):
        return self.__log
    
    @fn_log.setter
    def fn_log(self, log):
        self.__log = list({_dict['source']: _dict for _dict in log}.values())
    
    def check_success_files(call_func):
        def fn_success_files(self):
            
            logging.info('Check Success Files')
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
            else:
                logging.info(f"File Found Count {len(success_file)} Status: Success")
                
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
    
    def mapping_data(call_func):
        def fn_data_mapping(self):
            for _dict in call_func(self):
                try:
                    for sheets, data in _dict['data'].items():
                        logging.info(f"Mapping Data Sheet: '{sheets}'")
                        df = pd.DataFrame(data)
                        df.columns = df.iloc[0]
                        df = df[1:]
                        df = df.reset_index(drop=True)
                        
                except Exception as err:
                    _dict.update({'errors': str(err)})
                    
                #_dict.update({'Mapping': 'Success'})
            
            return self.fn_log
        
        return fn_data_mapping
    
    @mapping_data
    def get_data_files(self):
        
        logging.info('Get Data Files')
        for _dict in self.fn_log:
            full_path = _dict['full_path']
            types = Path(_dict['full_path']).suffix
            
            try:
                if ['.xlsx', '.xls'].__contains__(types):
                    logging.info(f"Read Excel Files: '{full_path}'")
                    list_data = verify_files.generate_excel_dataframe(full_path)
                else:
                    logging.info(f"Read Text Files: '{full_path}'")
                    list_data = verify_files.generate_text_dataframe(full_path)
                    
                _dict.update({'data': list_data})
                
            except Exception as err:
                _dict.update({'errors': err})
                
        if 'errors' in self.fn_log[0]:
            raise CustomException(self.fn_log)
        
        return self.fn_log
    
    def write_to_file(self):
        
        logging.info('Write Data to Files')
        date = datetime.datetime.now().strftime('%Y%m%d')
        excel = f"{FOLDER.EXCEL}excel_{date}.xlsx" 
        wb = openpyxl.Workbook()
        wb.active
        
        for _dict in self.fn_log:
            try:
                for name, data in _dict['data'].items():  
                    df = pd.DataFrame(data)
                    ## Write Excel
                    if self.output == 1:
                        sheet = wb.create_sheet(name)
                        rows = dataframe_to_rows(df, index=False , header=True)
                        for rdx, row in enumerate(rows, 1):
                            for cdx, val in enumerate(row, 1):
                                wf = sheet.cell(row=rdx, column=cdx)
                                wf.value = val
                        wb.save(excel)
                        
                    ## Write CSV
                    elif self.output == 2:
                        df = pd.DataFrame(data)
                        csv_name = f"{FOLDER.CSV}{name}{date}.csv"
                        df.to_csv(csv_name, index=False, float_format='%g')
                        
                    ## Write Text
                    else:
                        print("OK")
                    
            except Exception as err:
                _dict.update({'errors': str(err)})
                    
        if 'errors' in self.fn_log[0]:
            raise CustomException(self.fn_log)
        