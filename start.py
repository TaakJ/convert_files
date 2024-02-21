import glob
import warnings
import datetime
import openpyxl
import logging
import pandas as pd
from pathlib import Path
from openpyxl.utils.dataframe import dataframe_to_rows
from verify import verify_files

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

class convert_2_file(verify_files):
    def __init__(self, *args, **kwargs):
        super().__init__()
        
        for key, value in kwargs.items():
            setattr(self, key, value)
            
        self.get_list_files()
        self.get_data_files()
        self.compare_data_to_file()
        self.write_to_file()
        
    @property
    def fn_log(self):
        return self.__log
    
    @fn_log.setter
    def fn_log(self, log):
        self.__log = list({_dict['source']: _dict for _dict in log}.values())
    
    def check_success_files(call_method):
        def fn_success_files(self):
            logging.info('Check Success Files')
            success_file = []
            for _dict in call_method(self):
                filename = _dict['full_path']
                
                full_path = self.RAW + filename
                if glob.glob(full_path, recursive=True):
                    status = 'Success'
                    success_file.append(status)
                else:
                    status = 'Missing'
                    success_file.append(status)
                _dict.update({'full_path': full_path, 'status': status})
                
            if success_file.__contains__('Missing'):
                raise CustomException(self.fn_log)
            else:
                logging.info(f"File Found Count {len(success_file)} Status: Success")
                
            return self.fn_log
        
        return fn_success_files
    
    @check_success_files
    def get_list_files(self):
        fn_log = []
        for file in self.TEMPLATE:
            source = Path(file).stem
            fn_log.append({'source': source, 'full_path': file})
        self.fn_log = fn_log
        
        return self.fn_log
    
    def sample(call_method):
        logging.info("Mock Data")
        def fn_mock_data(self):
            mock_data = [['ApplicationCode',	'AccountOwner', 'AccountName',	'AccountType',	'EntitlementName',	'SecondEntitlementName','ThirdEntitlementName', 'AccountStatus',	'IsPrivileged',	'AccountDescription',
                        'CreateDate',	'LastLogin','LastUpdatedDate',	'AdditionalAttribute'], 
                        [1,2,3,4,5,6,7,8,9,10,self.date.strftime('%Y-%m-%d'),12,self.date.strftime('%Y-%m-%d %H:%M:%S'),14],
                        # [15,16,17,18,19,20,21,22,23,24,self.date.strftime('%Y-%m-%d'),26,self.date.strftime('%Y-%m-%d %H:%M:%S'),28],
                        # [29,30,31,32,33,34,35,36,37,38,self.date.strftime('%Y-%m-%d'),40,self.date.strftime('%Y-%m-%d %H:%M:%S'),42],
                        # [43,44,45,46,47,48,49,50,51,52,self.date.strftime('%Y-%m-%d'),54,self.date.strftime('%Y-%m-%d %H:%M:%S'),56],
                        # [57,58,59,60,61,62,63,64,65,66,self.date.strftime('%Y-%m-%d'),68,self.date.strftime('%Y-%m-%d %H:%M:%S'),70]
                        ]
            df = pd.DataFrame(mock_data)
            df.columns = df.iloc[0].values
            df = df[1:]
            df = df.reset_index(drop=True)
            call_method(self).append({'source': 'write', 'data': df.to_dict('list')})
            
        return fn_mock_data
    
    @sample
    def get_data_files(self):
        logging.info('Get Data Files')
        for _dict in self.fn_log:
            full_path = _dict['full_path']
            types = Path(_dict['full_path']).suffix
            
            try:
                if ['.xlsx', '.xls'].__contains__(types):
                    logging.info(f"Read Excel Files: '{full_path}'")
                    list_data = self.generate_excel_dataframe(full_path)
                else:
                    logging.info(f"Read Text Files: '{full_path}'")
                    list_data = self.generate_text_dataframe(full_path)
                _dict.update({'data': list_data})
                
            except Exception as err:
                _dict.update({'errors': err})
                
        if 'errors' in self.fn_log[0]:
            raise CustomException(self.fn_log)
        
        return self.fn_log
    
    def compare_data_to_file(self):
        logging.info('Compare Data to Files')
        csv_name = f"{self.LOG}DD_{self.date.strftime('%d%m%Y')}.csv"
        
        try:
            for _dict in self.fn_log:
                if _dict['source'] == 'write':
                    ## new data record
                    df_new = pd.DataFrame( _dict['data'])
                    
                    if glob.glob(csv_name, recursive=True):
                        ## old data record
                        df_csv = pd.read_csv(csv_name)
                
                    
        except Exception as err:
            print(f"err: {err}")
            
    def write_to_file(self):
        print(self.date)