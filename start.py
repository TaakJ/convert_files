import glob
import warnings
import openpyxl
import logging
import pandas as pd
from pathlib import Path
from openpyxl.utils.dataframe import dataframe_to_rows
from verify import verify_files

class CustomException(Exception):
    def __init__(self, err_):
        self.n = 0
        self.msg_err = self.generate_meg_err(err_)
        
    def __iter__(self):
        return self
    
    def __next__(self):
        return next(self.msg_err)
    
    def generate_meg_err(self, err_):
        for i in  range(len(err_)):
            msg_err = f"Filename: '{err_[i]['full_path']}' Status: '{err_[i]['status']}' Error: '{err_[i].get('errors')}'"
            if err_[i]['status'] == 'Success':
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
        # self.write_to_file()
        
    @property
    def getting_log(self):
        return self.__log
    
    @getting_log.setter
    def getting_log(self, log):
        self.__log = list({key['source']: key for key in log}.values())
    
    def check_success_files(call_method):
        def fn_success_files(self):
            logging.info('Check Success Files')
            success_file = []
            for key in call_method(self):
                filename = key['full_path']
                
                full_path = self.RAW + filename
                if glob.glob(full_path, recursive=True):
                    status = 'Success'
                    success_file.append(status)
                else:
                    status = 'Missing'
                    success_file.append(status)
                key.update({'full_path': full_path, 'status': status})
                
            if success_file.__contains__('Missing'):
                raise CustomException(self.getting_log)
            else:
                logging.info(f"File Found Count {len(success_file)} Status: Success")
                
            return self.getting_log
        
        return fn_success_files
    
    @check_success_files
    def get_list_files(self):
        getting_log = []
        for file in self.TEMPLATE:
            source = Path(file).stem
            getting_log.append({'source': source, 'full_path': file})
        self.getting_log = getting_log
        
        return self.getting_log
    
    def sample(call_method):
        
        logging.info("Mock Data")
        def fn_mock_data(self):
            mock_data = [['ApplicationCode',	'AccountOwner', 'AccountName',	'AccountType',	'EntitlementName',	'SecondEntitlementName','ThirdEntitlementName', 'AccountStatus',	'IsPrivileged',	'AccountDescription',
                        'CreateDate',	'LastLogin','LastUpdatedDate',	'AdditionalAttribute'], 
                        [10,2,3,4,5,6,7,8,9,10,self.date.strftime('%Y-%m-%d'),12,self.date.strftime('%Y-%m-%d %H:%M:%S'),14],
                        [150,16,17,18,19,20,21,22,23,24,self.date.strftime('%Y-%m-%d'),26,self.date.strftime('%Y-%m-%d %H:%M:%S'),28],
                        [290,30,31,32,33,34,35,36,37,38,self.date.strftime('%Y-%m-%d'),40,self.date.strftime('%Y-%m-%d %H:%M:%S'),42],
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
        for key in self.getting_log:
            full_path = key['full_path']
            types = Path(key['full_path']).suffix
            
            try:
                if ['.xlsx', '.xls'].__contains__(types):
                    logging.info(f"Read Excel Files: '{full_path}'")
                    list_data = self.generate_excel_dataframe(full_path)
                else:
                    logging.info(f"Read Text Files: '{full_path}'")
                    list_data = self.generate_text_dataframe(full_path)
                key.update({'data': list_data})
                
            except Exception as err:
                key.update({'errors': err})
                
        if 'errors' in self.getting_log[0]:
            raise CustomException(self.getting_log)
        
        return self.getting_log
    
    def compare_data_to_file(self):
        logging.info('Compare Data to Files')
        csv_name = f"{self.LOG}DD_{self.date.strftime('%d%m%Y')}.csv"
        
        try:
            for key in self.getting_log:
                if key['source'] == 'write':
                    ## new data record
                    new_df = pd.DataFrame( key['data'])
                    
                    if glob.glob(csv_name, recursive=True):
                        ## old data record
                        csv_df = pd.read_csv(csv_name)
                        self.generate_tmp_dataframe(csv_df, new_df)
                    else:
                        new_df.to_csv(csv_name, index=False, header=True)
        
        except Exception as err:
            print(f"err: {err}")
            
    def write_to_file(self):
        logging.info('Write Data to Files')
        print(self.date)