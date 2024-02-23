import glob
import warnings
import openpyxl
import csv
import logging
import pandas as pd
from pathlib import Path
from openpyxl.utils.dataframe import dataframe_to_rows
from verify import verify_files

class CustomException(Exception):
    def __init__(self, err_):
        self.n = 0
        
        # for key, value in kwargs.items():
        #     setattr(self, key, value)
        
        self.msg_err = self.generate_meg_err(err_)
        
    def __iter__(self):
        return self
    
    def __next__(self):
        return next(self.msg_err)
    
    def generate_meg_err(self, err_):
        for i in  range(len(err_)):
            # msg_err = f"Filename: '{err_[i]['full_path']}' Status: '{err_[i]['status']}' Error: '{err_[i].get('errors')}'"
            # if err_[i]['status'] == 'success':
            #     self.n += 1
            msg_err = err_[i].get('errors')
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
    def logging(self):
        return self.__log
    
    @logging.setter
    def logging(self, log):
        self.__log = list({key['source']: key for key in log}.values())
    
    def check_success_files(call_method):
        def fn_success_files(self):
            
            logging.info('check success files')
            success_file = []
            
            for key in call_method(self):
                filename = key['full_path']
                
                full_path = self.RAW + filename
                if glob.glob(full_path, recursive=True):
                    status = 'successed'
                    success_file.append(status)
                else:
                    status = 'missing'
                    success_file.append(status)
                key.update({'full_path': full_path, 'status': status})
                
            if success_file.__contains__('missing'):
                raise CustomException(self.logging)
            else:
                logging.info(f"file found count {len(success_file)} status: success")
                
            return self.logging
        
        return fn_success_files
    
    @check_success_files
    def get_list_files(self):
        
        log = [] 
        for file in self.TEMPLATE:
            source = Path(file).stem
            log.append({'source': source, 'full_path': file})
        self.logging = log
        
        return self.logging
    
    def sample(call_method):
        
        logging.info("Mock Data")
        def fn_mock_data(self):
            mock_data = [['ApplicationCode',	'AccountOwner', 'AccountName',	'AccountType',	'EntitlementName',	'SecondEntitlementName','ThirdEntitlementName', 'AccountStatus',	'IsPrivileged',	'AccountDescription',
                        'CreateDate',	'LastLogin','LastUpdatedDate',	'AdditionalAttribute'], 
                        [1,2,3,4,5,6,7,8,9,10,self.date.strftime('%Y-%m-%d'),12,self.date.strftime('%Y-%m-%d %H:%M:%S'),14],
                        [15,16,17,18,19,20,21,22,23,24,self.date.strftime('%Y-%m-%d'),26,self.date.strftime('%Y-%m-%d %H:%M:%S'),28],
                        # [29,30,31,32,33,34,35,36,37,38,self.date.strftime('%Y-%m-%d'),40,self.date.strftime('%Y-%m-%d %H:%M:%S'),42],
                        # [43,44,45,46,47,48,49,50,51,52,self.date.strftime('%Y-%m-%d'),54,self.date.strftime('%Y-%m-%d %H:%M:%S'),56],
                        # [57,58,59,60,61,62,63,64,65,66,self.date.strftime('%Y-%m-%d'),68,self.date.strftime('%Y-%m-%d %H:%M:%S'),70]
                        ]
            df = pd.DataFrame(mock_data)
            df.columns = df.iloc[0].values
            df = df[1:]
            df = df.reset_index(drop=True)
            call_method(self).append({'source': 'Target_file', 'data': df.to_dict('list')})
            
        return fn_mock_data
    
    @sample
    def get_data_files(self):
        
        logging.info('get data from files')
        status = 'failed'
        
        for key in self.logging:
            full_path = key['full_path']
            types = Path(key['full_path']).suffix
            key.update({'status': status})
            
            try:
                if ['.xlsx', '.xls'].__contains__(types):
                    logging.info(f"read excel files: '{full_path}'")
                    list_data = self.generate_excel_dataframe(full_path)
                else:
                    logging.info(f"read text files: '{full_path}'")
                    list_data = self.generate_text_dataframe(full_path)
                    
                status = 'successed'
                key.update({'data': list_data, 'status': status})
                
            except Exception as err:
                key.update({'errors': err})
                
            if 'errors' in key:
                raise CustomException(self.logging)
        
        return self.logging
    
    def compare_data_to_file(self):
        
        logging.info('compare data to files')
        csv_name = f"{self.LOG}DD_{self.date.strftime('%d%m%Y')}.csv"
        status = 'failed'
        
        for key in self.logging:
            try:
                if key['source'] == 'Target_file':
                    new_df = pd.DataFrame( key['data'])
                    key.update({'full_path': csv_name, 'status': status})
                    
                    if glob.glob(csv_name, recursive=True):
                        csv_df = pd.read_csv(csv_name)
                        to_update = self.generate_tmp_dataframe(csv_df, new_df)
                        
                        ## read from file
                        with open(csv_name, 'r') as reader:
                            csvin = csv.DictReader(reader, skipinitialspace=True)
                            rows = {idx: rows for idx, rows in enumerate(csvin)}
                            for idx in to_update:
                                if idx in rows:
                                    rows[idx].update(to_update[idx])
                                    logging.info(f"update record num: {idx}, data: {rows[idx]}")
                                else:
                                    rows.update({idx: to_update[idx]})
                                    logging.info(f"insert record num: {idx}, data: {rows[idx]}")
                                    
                        ## write to file
                        with open(csv_name, 'w', newline='') as writer:
                            csvout = csv.DictWriter(writer, csvin.fieldnames)
                            csvout.writeheader()
                            for idx in rows:
                                if idx not in self.skip_rows:
                                    csvout.writerow(rows[idx])
                        writer.closed
                        status = 'successed'
                    else:
                        new_df.to_csv(csv_name, index=False, header=True)    
                        status = 'successed'     
                        
                    key.update({'status': status})
                    logging.info(f"write to tmp file status: {status}")
                    
            except Exception as err:
                key.update({'errors': err})
        
            if 'errors' in key:
                raise CustomException(self.logging)
            
    def write_to_file(self):
        
        logging.info("write data to target files")
        target_name = f"{self.EXPORT}Application Data Requirements.xlsx"
        
        for key in self.logging:
            if key['source'] == 'Target_file':
                filename = key['full_path']
                status = key['status']
                
                if status == 'successed':
                    csv_df = pd.read_csv(filename)
                    self.generate_target_dataframe(csv_df)
                else:
                    raise CustomException(self.logging)
                
                try:
                    status = 'failed'
                    key.update({'full_path': target_name, 'status': status})
                    
                    ## write to file
                    print(target_name)
                    
                except Exception as err:
                    print(err)