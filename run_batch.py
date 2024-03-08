import glob
import warnings
import logging
import pandas as pd
from pathlib import Path
from verify_data import validate_files
from exception import CustomException
from datetime import datetime
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font


class convert_2_file(validate_files):
    def __init__(self, **kwargs):
        super().__init__()

        for key, value in kwargs.items():
            setattr(self, key, value)

        self.__log = []
        self.get_list_files()
        self.get_data_files()
        self.write_data_to_tmp_file()
        self.write_data_to_target_file()

    @property
    def logging(self):
        return self.__log

    @logging.setter
    def logging(self, log):
        self.__log = log

    def check_success_files(func):
        def wrapper_success_files(*args):

            logging.info('Check Success files..')

            success_file = []
            for key in func(*args):
                filename = key['full_path']
                full_path = args[0].RAW + filename

                if glob.glob(full_path, recursive=True):
                    status = 'successed'
                    success_file.append(status)
                else:
                    status = 'missing'
                    success_file.append(status)
                key.update({'full_path': full_path, 'status': status})

            if success_file.__contains__('missing'):
                raise CustomException(func(*args))
            else:
                logging.info(f"File found count {len(success_file)} status: successed.")

            return func(*args)
        return wrapper_success_files

    @check_success_files
    def get_list_files(self):
        if self.__log == []:
            args = []
            for file in self.TEMPLATE:
                source = Path(file).stem
                args.append({'source': source, 'full_path': file})
            self.__log = args

        return self.__log

    def mock_data(func):

        logging.info("Mock Data")

        def wrapper_mock_data(*args):
            now = datetime.now()
            mock_data = [['ApplicationCode',	'AccountOwner', 'AccountName',	'AccountType',	'EntitlementName',	'SecondEntitlementName','ThirdEntitlementName', 'AccountStatus',	'IsPrivileged',	'AccountDescription',
                        'CreateDate',	'LastLogin','LastUpdatedDate',	'AdditionalAttribute'],
                        [1,2,3,4,5,6,7,8,9,10,args[0].date.strftime('%Y-%m-%d'),12, now.strftime('%Y-%m-%d %H:%M:%S'),14],
                        [15,16,17,18,19,20,21,22,23,24,args[0].date.strftime('%Y-%m-%d'),26, now.strftime('%Y-%m-%d %H:%M:%S'),28],
                        # [29,30,31,32,33,34,35,36,37,38,args[0].date.strftime('%Y-%m-%d'),40, now.strftime('%Y-%m-%d %H:%M:%S'),42],
                        # [43,44,45,46,47,48,49,50,51,52,args[0].date.strftime('%Y-%m-%d'),54,now.strftime('%Y-%m-%d %H:%M:%S'),56],
                        # [57,58,59,60,61,62,63,64,65,66,args[0].date.strftime('%Y-%m-%d'),68,now.strftime('%Y-%m-%d %H:%M:%S'),70],
                        # [71,72,73,74,75,76,77,78,79,80,args[0].date.strftime('%Y-%m-%d'),82,now.strftime('%Y-%m-%d %H:%M:%S'),83],
                        # [84,85,86,87,88,89,90,91,92,93,args[0].date.strftime('%Y-%m-%d'),95,now.strftime('%Y-%m-%d %H:%M:%S'),96],
                        ]
            df = pd.DataFrame(mock_data)
            df.columns = df.iloc[0].values
            df = df[1:]
            df = df.reset_index(drop=True)
            func(*args).append({'source': 'Target_file', 'data': df.to_dict('list')})

        return wrapper_mock_data
    
    @mock_data
    def get_data_files(self):

        logging.info('Get Data from files..')

        status = 'failed'
        for key in self.__log:
            full_path = key['full_path']
            types = Path(key['full_path']).suffix
            key.update({'status': status})
            
            try:
                if ['.xlsx', '.xls'].__contains__(types):
                    logging.info(f"Read Excel files: '{full_path}'.")
                    clean_data = self.generate_excel_data(full_path=full_path)
                else:
                    logging.info(f"Read Text files: '{full_path}'.")
                    clean_data = self.generate_text_data(full_path=full_path)
                status = 'successed'
                key.update({'data': clean_data, 'status': status})
                
            except Exception as err:
                key.update({'errors': err})

            if 'errors' in key:
                raise CustomException(self.__log)

        return self.__log

    def write_data_to_tmp_file(self):

        logging.info("Write Data to Tmp files..")

        tmp_name = f"{self.TMP}DD_{self.date.strftime('%d%m%Y')}.xlsx"
        workbook = openpyxl.Workbook()
        status = 'failed'
        start_rows = 2

        for key in self.__log:
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
                        workbook.active = sheet_num

                        tmp_df = pd.read_excel(tmp_name, sheet_name=sheet_name)
                        tmp_df = tmp_df.loc[tmp_df['recoreded'] != 'Removed']

                        ## compare data new data with tmp data
                        new_df = self.validation_data(tmp_df, new_df)

                        ## write to tmp files
                        sheet_name = f'RUN_TIME_{sheet_num + 1}'
                        sheet = workbook.create_sheet(sheet_name)
                        logging.info(f"Genarate Sheet_name: {sheet_name} in Tmp files.")

                        header =  [columns for columns in new_df[start_rows].keys()]
                        sheet.append(header)
                        while start_rows <= max(new_df):
                            for recoreded in [new_df[start_rows][columns] for columns in new_df[start_rows].keys() if columns == 'recoreded']:
                                for idx, values in enumerate(new_df[start_rows].values(), 1):
                                    if start_rows in self.skip_rows and recoreded == 'Removed':
                                        sheet.cell(row=start_rows, column=idx).value = values
                                        sheet.cell(row=start_rows, column=idx).font = Font(bold=True, strike=True, color="00FF0000")
                                        show = f"{recoreded} Rows: ({start_rows}) in Tmp files."
                                    elif start_rows in self.diff_rows.keys() and recoreded in ['Updated', 'Inserted']:
                                        sheet.cell(row=start_rows, column=idx).value = values
                                        show = f"{recoreded} Rows: ({start_rows}) in Tmp files. Record Changed: {self.diff_rows[start_rows]}"
                                    else:
                                        sheet.cell(row=start_rows, column=idx).value = values
                                        show = f"No Change Rows: ({start_rows}) in Tmp files."
                                logging.info(show)
                            start_rows += 1
                    except FileNotFoundError:
                        ## write tmp files on first time.
                        sheet_name = 'RUN_TIME_1'
                        sheet = workbook.worksheets[0]
                        sheet_num = 1
                        sheet.title = sheet_name

                        rows = dataframe_to_rows(new_df, header=True, index=False)
                        for r_idx, row in enumerate(rows, 1):
                            for c_idx, value in enumerate(row, 1):
                                sheet.cell(row=r_idx, column=c_idx).value = value
                            logging.info(f"Inserted Rows: ({r_idx}) in Tmp files.")
                    
                    workbook.move_sheet(workbook.active, offset = -sheet_num)
                    workbook.save(tmp_name)
                    
                    status = 'successed'

                    key.update({'sheet_name': sheet_name  ,'status': status})
                    logging.info(f"Write to Tmp files status: {status}.")

            except Exception as err:
                key.update({'errors': err})

            if 'errors' in key:
                raise CustomException(self.__log)

    def write_data_to_target_file(self):

        logging.info("Write Data to Target files..")

        target_name = f"{self.EXPORT}Application Data Requirements.xlsx"
        status = 'failed'
        start_rows = 2

        def remove_row_empty(sheet):
            for row in sheet.iter_rows():
                if not all(cell.value for cell in row):
                    sheet.delete_rows(row[0].row, 1)
                    remove_row_empty(sheet)

        for key in self.__log:
            try:
                if key['source'] == 'Target_file':
                    try:
                        ## read target file
                        target_df = pd.read_excel(target_name)
                        target_df['recoreded'] = 'Inserted'

                        ## read tmp file
                        tmp_name = key['full_path']
                        sheet_name = key['sheet_name']
                        tmp_df = pd.read_excel(tmp_name, sheet_name=sheet_name)
                        tmp_df = tmp_df.loc[tmp_df['recoreded'] != 'Removed']

                        ## compare data tmp data with target data
                        if not target_df.empty:
                            select_date = tmp_df['CreateDate'].unique()
                            new_df = self.append_target_data(select_date, target_df, tmp_df)
                        else:
                            tmp_df = tmp_df.to_dict('index')
                            new_df = {start_rows + key: value for key,value in tmp_df.items()}
                            
                        key.update({'full_path': target_name, 'status': status})

                    except Exception as err:
                        key.update({'errors': err})

                    ## write data to target file
                    workbook = openpyxl.load_workbook(target_name)
                    get_sheet = workbook.get_sheet_names()
                    sheet = workbook.get_sheet_by_name(get_sheet[0])
                    workbook.active

                    while start_rows <= max(new_df):
                        for idx, columns in enumerate(new_df[start_rows].keys(), 1):
                            if columns == 'recoreded':
                                if start_rows in self.diff_rows.keys() and new_df[start_rows][columns] in ['Updated', 'Inserted']:
                                    show = f"{new_df[start_rows][columns]} Rows: ({start_rows}) in Target files. Record Changed: {self.diff_rows[start_rows]}"
                                elif start_rows in self.skip_rows and new_df[start_rows][columns] == 'Removed':
                                    show = f"{new_df[start_rows][columns]} Rows: ({start_rows}) in Target files."
                                    sheet.delete_rows(start_rows,sheet.max_row)
                                else:
                                    show = f"No Change Rows: ({start_rows}) in Target files."
                            else:
                                if start_rows in self.skip_rows:
                                    continue
                                sheet.cell(row=start_rows, column=idx).value = new_df[start_rows][columns]
                                continue
                            logging.info(show)
                        start_rows += 1
                    remove_row_empty(sheet)
                    
                    workbook.save(target_name)
                    status = 'successed'
                    key.update({'status': status})
                    logging.info(f"Write to Target Files status: {status}.")

            except Exception as err:
                key.update({'errors': err})

        if 'errors' in key:
            raise CustomException(self.__log)
