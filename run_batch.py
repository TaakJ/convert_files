import glob
import warnings
import csv
import logging
import openpyxl
import pandas as pd
from pathlib import Path
from verify_data import validate_files
from exception import CustomException
from datetime import datetime

class convert_2_file(validate_files):
    def __init__(self, **kwargs):
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
        # self.__log = list({key['source']: key for key in log}.values())
        self.__log = log
    
    def check_success_files(call_method):
        def fn_success_files(self):
            
            logging.info('Check Success files..')
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
                logging.info(f"\033[1mFile found count {len(success_file)} status: successed.\033[0m")
                
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
            now = datetime.now()
            mock_data = [['ApplicationCode',	'AccountOwner', 'AccountName',	'AccountType',	'EntitlementName',	'SecondEntitlementName','ThirdEntitlementName', 'AccountStatus',	'IsPrivileged',	'AccountDescription',
                        'CreateDate',	'LastLogin','LastUpdatedDate',	'AdditionalAttribute'], 
                        [1,2,3,4,5,6,7,8,9,10,self.date.strftime('%Y-%m-%d'),12, now.strftime('%Y-%m-%d %H:%M:%S'),14],
                        [15,16,17,18,19,20,21,22,23,24,self.date.strftime('%Y-%m-%d'),26, now.strftime('%Y-%m-%d %H:%M:%S'),28],
                        # [29,30,31,32,33,34,35,36,37,38,self.date.strftime('%Y-%m-%d'),40, now.strftime('%Y-%m-%d %H:%M:%S'),42],
                        # [43,44,45,46,47,48,49,50,51,52,self.date.strftime('%Y-%m-%d'),54,now.strftime('%Y-%m-%d %H:%M:%S'),56],
                        # [57,58,59,60,61,62,63,64,65,66,self.date.strftime('%Y-%m-%d'),68,now.strftime('%Y-%m-%d %H:%M:%S'),70]
                        ]
            df = pd.DataFrame(mock_data)
            df.columns = df.iloc[0].values
            df = df[1:]
            df = df.reset_index(drop=True)
            call_method(self).append({'source': 'Target_file', 'data': df.to_dict('list')})
            
        return fn_mock_data
    
    @sample
    def get_data_files(self):
        
        logging.info('Get Data from files..')
        status = 'failed'
        
        for key in self.logging:
            full_path = key['full_path']
            types = Path(key['full_path']).suffix
            key.update({'status': status})
            
            try:
                if ['.xlsx', '.xls'].__contains__(types):
                    logging.info(f"Read Excel files: '{full_path}'.")
                    list_data = self.generate_excel_data(full_path)
                    
                else:
                    logging.info(f"Read Text files: '{full_path}'.")
                    list_data = self.generate_text_data(full_path)
                    
                status = 'successed'
                key.update({'data': list_data, 'status': status})
                
            except Exception as err:
                key.update({'errors': err})
                
            if 'errors' in key:
                raise CustomException(self.logging)
        
        return self.logging
    
    def compare_data_to_file(self):
        
        logging.info('Compare Data to Tmp files..')
        csv_name = f"{self.LOG}DD_{self.date.strftime('%d%m%Y')}.csv"
        status = 'failed'
        
        for key in self.logging:
            try:
                if key['source'] == 'Target_file':
                    new_df = pd.DataFrame( key['data'])
                    key.update({'full_path': csv_name, 'status': status})
                    
                    if glob.glob(csv_name, recursive=True):
                        tmp_df = pd.read_csv(csv_name)
                        output = self.check_up_data(tmp_df, new_df)
                        
                        ## read from file
                        start_rows = 2 
                        with open(csv_name, 'r') as reader:
                            csvin = csv.DictReader(reader, skipinitialspace=True)
                            rows = {idx: rows for idx, rows in enumerate(csvin)}
                            if output != {}:
                                for idx in output:
                                    if idx in rows:
                                        if idx not in self.skip_rows:
                                            
                                            change_value = {}
                                            for value in rows[idx]: 
                                                if value in output[idx] and (str(rows[idx][value]) != str(output[idx][value])):
                                                    change_value.update({value: f"{rows[idx][value]} => {output[idx][value]}"})
                                                    rows[idx].update({value: output[idx][value]})
                                            logging.info(f"\033[1mUpdated Rows: {idx + start_rows} in Tmp files. Recorded: {change_value}\033[0m")
                                            
                                        else:
                                            logging.info(f"\033[1mDeleted Rows: {idx + start_rows} in Tmp files.\033[0m")
                                            continue
                                    else:
                                        rows.update({idx: output[idx]})
                                        logging.info(f"\033[1mInserted Rows: {idx + start_rows} in Tmp files. Recorded: {rows[idx]}\033[0m")
                            else:
                                logging.info("\033[1mNo changes data in Tmp files.\033[0m")
                                
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
                        logging.info(f"\033[1mCreate Tmp files '{csv_name}.'\033[0m")
                        
                    key.update({'status': status})
                    logging.info(f"Write to Tmp files status: {status}.")
                    
            except Exception as err:
                key.update({'errors': err})
            
            if 'errors' in key:
                raise CustomException(self.logging)
            
    def write_to_file(self):
        
        logging.info("Write Data to Target files..")
        target_name = f"{self.EXPORT}Application Data Requirements.xlsx"
        
        workbook = openpyxl.load_workbook(target_name)
        get_sheet = workbook.get_sheet_names()
        sheet = workbook.get_sheet_by_name(get_sheet[0])
        workbook.active
        
        for key in self.logging:
            try:
                status = 'failed'
                if key['source'] == 'Target_file':
                    filename = key['full_path']
                    status = key['status']
                    
                    if status == 'successed':
                        tmp_df = pd.read_csv(filename)
                        output = self.get_data_target(target_name, tmp_df)
                    else:
                        raise CustomException(self.logging)
                    key.update({'full_path': target_name, 'status': status})
                    
                    ## write data to target file
                    start_rows = 2
                    while start_rows <= max(output):
                        if start_rows in self.insert_rows:
                            for idx, value in enumerate(output[start_rows].values(), 1):
                                sheet.cell(row=start_rows, column=idx).value = value
                            logging.info(f"\033[1mWrote Rows: {start_rows} in Target files. Recorded: {output[start_rows]}\033[0m")
                        
                        else:
                            for idx, value in enumerate(output[start_rows].values(), 1):
                                sheet.cell(row=start_rows, column=idx).value = value   
                        start_rows += 1
                    
                    ## check deleted rows
                    if len(self.skip_rows) != 0:
                        rows = max(output) + 1
                        logging.info(f"\033[1mDeleted Rows: {rows} to {sheet.max_row} tp  in Target files.\033[0m")
                        sheet.delete_rows(idx=rows, amount=len(self.skip_rows))
                    
                    workbook.save(target_name)
                    status = 'successed'
                    
                    key.update({'status': status})
                    logging.info(f"write to target files status: {status}.")
                    
            except Exception as err:
                key.update({'errors': err})
                
        if 'errors' in key:
            raise CustomException(self.logging)