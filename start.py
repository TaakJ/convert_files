import glob
import warnings
import datetime
import openpyxl
import logging
import pandas as pd
import numpy as np
import csv
from datetime import datetime
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
        
        self.date = datetime.now()
        self.get_list_files()
        self.get_data_files()
        self.compare_data_to_file()
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
                
            if success_file.__contains__('Missing'):
                raise CustomException(self.fn_log)
            else:
                logging.info(f"File Found Count {len(success_file)} Status: Success")
                
            return self.fn_log
        
        return fn_success_files
    
    
    @check_success_files
    def get_list_files(self):
        
        fn_log = []
        for file in self.template:
            source = Path(file).stem
            fn_log.append({'source': source, 'full_path': file})
        self.fn_log = fn_log
        
        return self.fn_log
    
    
    # def mapping_data(call_func):
    #     def fn_data_mapping(self):
    #         for _dict in call_func(self):
    #             try:
    #                 for sheets, data in _dict['data'].items():
    #                     logging.info(f"Mapping Data Sheet: '{sheets}'")
    #                     ## ignore UserWarning: Data Validation no header in sheet: USER REPORT
    #                     warnings.simplefilter(action='ignore', category=UserWarning)
    #                     df = pd.DataFrame(data)
    #                     df.columns = df.iloc[0].values
    #                     df = df[1:]
    #                     df = df.reset_index(drop=True)
    #                     _dict.update({'data': df.to_dict('list')})
                        
    #             except Exception as err:
    #                 _dict.update({'errors': str(err)})
                    
    #         if 'errors' in self.fn_log[0]:
    #             raise CustomException(self.fn_log)
                
    #         return self.fn_log
        
    #     return fn_data_mapping
    
    
    def sample(call_func):
        logging.info("Mock Data")
        def fn_mock_data(self):
            mock_data = [['ApplicationCode',	'AccountOwner', 'AccountName',	'AccountType',	'EntitlementName',	'SecondEntitlementName','ThirdEntitlementName', 'AccountStatus',	'IsPrivileged',	'AccountDescription',
                        'CreateDate',	'LastLogin','LastUpdatedDate',	'AdditionalAttribute'], 
                        [1,2,3,4,5,6,7,8,9,10,self.date.strftime('%Y-%m-%d'),12,self.date.strftime('%Y-%m-%d %H:%M:%S'),14],
                        # [15,16,17,18,19,20,21,22,23,24,self.date.strftime('%Y-%m-%d'),26,self.date.strftime('%Y-%m-%d %H:%M:%S'),28],
                        # [29,30,31,32,33,34,35,36,37,38,self.date.strftime('%Y-%m-%d'),40,self.date.strftime('%Y-%m-%d %H:%M:%S'),42],
                        # [43,44,45,46,47,48,49,50,51,52,self.date.strftime('%Y-%m-%d'),60,self.date.strftime('%Y-%m-%d %H:%M:%S'),62]
                        ]
            df = pd.DataFrame(mock_data)
            df.columns = df.iloc[0].values
            df = df[1:]
            df = df.reset_index(drop=True)
            call_func(self).append({'source': 'write', 'data': df.to_dict('list')})
            
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
    
    def compare_data_to_file(self):
        
        logging.info('Compare Data to Files')
        csv_name = f"{FOLDER.LOG}DD_{self.date.strftime('%d%m%Y')}.csv"
        
        try:
            
            for _dict in self.fn_log:
                if _dict['source'] == 'write':
                    ## new data record
                    df_new = pd.DataFrame( _dict['data'])
                    
                    if glob.glob(csv_name, recursive=True):
                        ## old data record
                        df_csv = pd.read_csv(csv_name)
                        
                        if df_csv.index[-1] <= df_new.index[-1]:
                            del_idx = []
                            record = "new"
                        else:
                            del_idx = [idx for idx in list(df_csv.index) if idx not in list(df_new.index)]
                            record = "remove"
                            
                        ## replace new / data to record
                        compare_df = pd.DataFrame(np.where(df_new.ne(df_csv), record, 'data'), columns=df_new.columns)
                        compare_df['count'] = compare_df.apply(lambda data: data.value_counts()[record], axis=1)
                        
                        for row_idx in [idx for idx in list(compare_df.index) if idx in list(df_new.index)]:
                            for (col_cmp, val_cmp), (_, val_new) in zip(compare_df.items(), df_new.items()):
                                    if val_cmp.iloc[row_idx] in ["data", "new"]:
                                        compare_df.at[row_idx, col_cmp] = val_new.iloc[row_idx]
                        compare_df = compare_df[compare_df.iloc[:,14] > 1].iloc[:,:-1]
                        
                        ## read from file
                        with open(csv_name, 'r') as reader:
                            csvin = csv.DictReader(reader, skipinitialspace=True)
                            to_update = {idx: rows for idx, rows in df_new.to_dict('index').items()}
                            from_update = {idx: rows for idx, rows in enumerate(csvin)}
                            
                            ## compare data
                            for row_idx in to_update:
                                if row_idx in from_update:
                                    ## update record
                                    from_update[row_idx].update(to_update[row_idx])
                                    logging.info(f"Update record num: {row_idx}, data: {from_update[row_idx]}")
                                else:
                                    ## insert record
                                    from_update.update({row_idx: to_update[row_idx]})
                                    logging.info(f"Insert record num: {row_idx}, data: {from_update[row_idx]}")
                        
                        ## write to file
                        with open(csv_name, 'w', encoding='UTF8', newline='') as writer:
                            csvout = csv.DictWriter(writer, csvin.fieldnames)
                            csvout.writeheader()
                            for row_idx in from_update:
                                ## check delete record
                                if row_idx not in del_idx:
                                    csvout.writerow(from_update[row_idx])
                    else:
                        df_new.to_csv(csv_name, index=False, header=True)
                    
        except Exception as err:
            print(f"test {err}")