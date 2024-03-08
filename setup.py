import logging.config
import shutil
import yaml
import os
from os.path import join
from datetime import datetime

CURRENT_DIR = os.getcwd()
LOGGER_CONFIG = join(CURRENT_DIR, 'logging_config.yaml')

class path_setup:
    
    RAW         = join(CURRENT_DIR, "raw/")
    EXPORT      = join(CURRENT_DIR, "export/")
    TMP         = join(CURRENT_DIR, "tmp/export/")
    LOG         = join(CURRENT_DIR, "tmp/log/")
    TEMPLATE    = ['ADM.txt', 'BOS.xlsx', 'CUM.xls', 'DocImage.txt', 'ICAS-NCR.xlsx', 'IIC.xlsx', 'LDS-P_UserDetail.txt', 'Lead-Management.xlsx', 'MOC.xlsx']

    @staticmethod
    def setup_log():
        config_yaml  = None
        date = datetime.today().strftime("%d%m%Y")
        log_name = f'log_{date}.log'
        
        if os.path.exists(LOGGER_CONFIG):
            with open(LOGGER_CONFIG, 'rb') as logger:
                config_yaml  = yaml.safe_load(logger.read())
                for i in (config_yaml["handlers"].keys()):
                    if 'filename' in config_yaml['handlers'][i]:
                        log_path = config_yaml["handlers"][i]["filename"]
                        log_file = log_path + log_name
                config_yaml["handlers"][i]["filename"] = log_file
                
                logging.config.dictConfig(config_yaml)
        else:
            raise Exception(f"Yaml file file_path: '{LOGGER_CONFIG}' doesn't exist")
            
    @staticmethod
    def setup_folder():
        _folders = [value for name, value in vars(path_setup).items() if isinstance(value, str) and not name.startswith('_')]
        for folder in _folders:
            os.makedirs(folder, exist_ok=True)

    @staticmethod
    def clear_folder():
        _folders = [value for name, value in vars(path_setup).items() if isinstance(value, str) and not name.startswith('_') and not value.endswith(('raw/','export/'))]
        for folder in _folders:
            shutil.rmtree(folder)

    # @staticmethod
    # def backup_folder():
    #     date = datetime.now().strftime('%d%m%Y')
    #     bk_path = join(FOLDER.EXPORT, f"BK_{date}")
    #     if not os.path.exists(bk_path):
    #         os.makedirs(bk_path)
    #     else:
    #         shutil.rmtree(bk_path)
    #         os.makedirs(bk_path)
    #     _folders = [value for name, value in vars(FOLDER).items() if isinstance(value, str) and not name.startswith('_') and value.endswith(('export/','log/'))]
    #     for folder in _folders:
    #         for files in os.listdir(folder):
    #             if files.endswith((".xlsx",'.log')):
    #                 shutil.copy2(join(folder, files), bk_path)
