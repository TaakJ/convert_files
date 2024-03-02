import glob
import warnings
import logging
import openpyxl
import pandas as pd
from pathlib import Path
from verify_data import validate_files
from exception import CustomException
from datetime import datetime
from openpyxl.utils.dataframe import dataframe_to_rows

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
                        [29,30,31,32,33,34,35,36,37,38,self.date.strftime('%Y-%m-%d'),40, now.strftime('%Y-%m-%d %H:%M:%S'),42],
                        [43,44,45,46,47,48,49,50,51,52,self.date.strftime('%Y-%m-%d'),54,now.strftime('%Y-%m-%d %H:%M:%S'),56],
                        [57,58,59,60,61,62,63,64,65,66,self.date.strftime('%Y-%m-%d'),68,now.strftime('%Y-%m-%d %H:%M:%S'),70]
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
        tmp_name = f"{self.LOG}DD_{self.date.strftime('%d%m%Y')}.xlsx"
        status = 'failed'
    
        workbook = openpyxl.Workbook()
        workbook.active 
        
        for key in self.logging:
            try:
                if key['source'] == 'Target_file':
                    new_df = pd.DataFrame( key['data'])
                    new_df['recoreded'] = 'Inserted'
                    
                    key.update({'full_path': tmp_name, 'status': status})
                    
                    try:
                        ## read tmp files
                        workbook = openpyxl.load_workbook(tmp_name)
                        get_sheet = workbook.get_sheet_names()
                        sheet_num = len(get_sheet)
                        sheet_name = f'RUN_TIME_{sheet_num}'
                        tmp_df = pd.read_excel(tmp_name, sheet_name=sheet_name)
                        tmp_df = tmp_df.loc[tmp_df['recoreded'] != 'Removed']
                        
                        ## compare data new data with tmp data
                        new_df = self.check_up_data(tmp_df, new_df)
                        
                        ## write to tmp files
                        logging.info(f"Genarate Sheet_name: {sheet_name} in Tmp files.")
                        sheet_name = f'RUN_TIME_{sheet_num + 1}'
                        sheet = workbook.create_sheet(sheet_name)
                        
                        start_rows = 2
                        header =  [columns for columns in new_df[start_rows].keys()]
                        sheet.append(header)
                        
                        while start_rows <= max(new_df):
                            for recoreded in [new_df[start_rows][columns] for columns in new_df[start_rows].keys() if columns == 'recoreded']:
                                for idx, value in enumerate(new_df[start_rows].values(), 1):
                                    sheet.cell(row=start_rows, column=idx).value = value
                                    
                                if start_rows in self.diff_rows.keys() and recoreded in ['Removed', 'Inserted']:
                                    show = f"{recoreded} Rows: \033[1m({start_rows})\033[0m in Tmp files. Record Changed: \033[1m{self.diff_rows[start_rows]}\033[0m"
                                    
                                elif start_rows in self.skip_rows and recoreded == 'Removed':
                                    show = f"{recoreded} Rows: \033[1m({start_rows})\033[0m in Tmp files."
                                    
                                else:
                                    show = "\033[1mNo changes data in Tmp files.\033[0m"
                                    
                                logging.info(show)
                            start_rows += 1
                        
                    except FileNotFoundError:
                        
                        ## write tmp files on first time. 
                        sheet_name = 'RUN_TIME_1'
                        sheet = workbook.worksheets[0]
                        sheet.title = sheet_name
                        
                        rows = dataframe_to_rows(new_df, header=True, index=False)
                        for r_idx, row in enumerate(rows, 1):
                            for c_idx, value in enumerate(row, 1):
                                sheet.cell(row=r_idx, column=c_idx).value = value
                        
                    workbook.save(tmp_name)
                    status = 'successed'
                    
                    key.update({'sheet_name': sheet_name  ,'status': status})
                    logging.info(f"Write to Tmp files status: {status}.")
                    
            except Exception as err:
                key.update({'errors': err})
            
            if 'errors' in key:
                raise CustomException(self.logging)
            
    def write_to_file(self):
        
        logging.info("Write Data to Target files..")
        target_name = f"{self.EXPORT}Application Data Requirements.xlsx"
        status = 'failed'
        
        workbook = openpyxl.load_workbook(target_name)
        get_sheet = workbook.get_sheet_names()
        sheet = workbook.get_sheet_by_name(get_sheet[0])
        workbook.active
        
        for key in self.logging:
            try:
                if key['source'] == 'Target_file':
                    tmp_name = key['full_path']
                    status = key['status']
                    sheet_name = key['sheet_name']
                    
                    key.update({'full_path': target_name, 'status': status})
                    
                    target_df = pd.read_excel(target_name)
                    if status == 'successed':
                        tmp_df = pd.read_excel(tmp_name, sheet_name=sheet_name)
                        tmp_df = tmp_df.loc[tmp_df['recoreded'] != 'Removed']
                    else:
                        raise FileNotFoundError(f"File Not Found: {tmp_name}")
                    
                    print(tmp_df)
                    
                    key.update({'full_path': target_name, 'status': status})
                    
                    # ## write data to target file
                    # start_rows = 2
                    # while start_rows <= max(output):
                    #     if start_rows in self.insert_rows:
                    #         for idx, value in enumerate(output[start_rows].values(), 1):
                    #             sheet.cell(row=start_rows, column=idx).value = value
                    #         logging.info(f"\033[1mWrote Rows: {start_rows} in Target files. Recorded: {output[start_rows]}\033[0m")
                        
                    #     else:
                    #         for idx, value in enumerate(output[start_rows].values(), 1):
                    #             sheet.cell(row=start_rows, column=idx).value = value   
                    #     start_rows += 1
                    
                    # ## check deleted rows
                    # if len(self.skip_rows) != 0:
                    #     rows = max(output) + 1
                    #     logging.info(f"\033[1mDeleted Rows: {rows} to {sheet.max_row} tp  in Target files.\033[0m")
                    #     sheet.delete_rows(idx=rows, amount=len(self.skip_rows))
                    
                    # workbook.save(target_name)
                    # status = 'successed'
                    
                    key.update({'status': status})
                    logging.info(f"write to target files status: {status}.")
                    
            except Exception as err:
                key.update({'errors': err})
                
        if 'errors' in key:
            raise CustomException(self.logging)
    
    
    # def write_to_file(self):
        
    #     logging.info("Write Data to Target files..")
    #     target_name = f"{self.EXPORT}Application Data Requirements.xlsx"
        
    #     workbook = openpyxl.load_workbook(target_name)
    #     get_sheet = workbook.get_sheet_names()
    #     sheet = workbook.get_sheet_by_name(get_sheet[0])
    #     workbook.active
        
    #     for key in self.logging:
    #         try:
    #             status = 'failed'
    #             if key['source'] == 'Target_file':
    #                 filename = key['full_path']
    #                 status = key['status']
                    
    #                 if status == 'successed':
    #                     tmp_df = pd.read_csv(filename)
    #                     output = self.get_data_target(target_name, tmp_df)
    #                 else:
    #                     raise CustomException(self.logging)
    #                 key.update({'full_path': target_name, 'status': status})
                    
    #                 ## write data to target file
    #                 start_rows = 2
    #                 while start_rows <= max(output):
    #                     if start_rows in self.insert_rows:
    #                         for idx, value in enumerate(output[start_rows].values(), 1):
    #                             sheet.cell(row=start_rows, column=idx).value = value
    #                         logging.info(f"\033[1mWrote Rows: {start_rows} in Target files. Recorded: {output[start_rows]}\033[0m")
                        
    #                     else:
    #                         for idx, value in enumerate(output[start_rows].values(), 1):
    #                             sheet.cell(row=start_rows, column=idx).value = value   
    #                     start_rows += 1
                    
    #                 ## check deleted rows
    #                 if len(self.skip_rows) != 0:
    #                     rows = max(output) + 1
    #                     logging.info(f"\033[1mDeleted Rows: {rows} to {sheet.max_row} tp  in Target files.\033[0m")
    #                     sheet.delete_rows(idx=rows, amount=len(self.skip_rows))
                    
    #                 workbook.save(target_name)
    #                 status = 'successed'
                    
    #                 key.update({'status': status})
    #                 logging.info(f"write to target files status: {status}.")
                    
    #         except Exception as err:
    #             key.update({'errors': err})
                
    #     if 'errors' in key:
    #         raise CustomException(self.logging)