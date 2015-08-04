import os
from typing import Dict, Union

logging_config_file_path = os.environ['LOGGING_CONFIG_FILE_PATH']
fault_log_file_path = os.environ['FAULT_LOG_FILE_PATH']
sqlalchemy_database = os.environ['SQLALCHEMY_DATABASE_URI']
index_config_file_path = os.environ['INDEX_CONFIG_FILE_PATH']
elasticsearch_uri = os.environ['ELASTICSEARCH_URI']
page_size = int(os.environ['PAGE_SIZE'])
polling_interval_secs = int(os.environ['POLLING_INTERVAL_SECS'])

CONFIG_DICT = {
    'DEBUG': False,
    'LOGGING': True,
    'LOGGING_CONFIG_FILE_PATH': logging_config_file_path,
    'FAULT_LOG_FILE_PATH': fault_log_file_path,
    'SQLALCHEMY_DATABASE_URI': sqlalchemy_database,
    'INDEX_CONFIG_FILE_PATH': index_config_file_path,
    'ELASTICSEARCH_URI': elasticsearch_uri,
    'PAGE_SIZE': page_size,
    'POLLING_INTERVAL_SECS': polling_interval_secs,
}  # type: Dict[str, Union[bool, str, int]]

settings = os.environ.get('SETTINGS')

if settings == 'dev':
    CONFIG_DICT['DEBUG'] = True
elif settings == 'test':
    CONFIG_DICT['LOGGING'] = False
    CONFIG_DICT['DEBUG'] = True
    CONFIG_DICT['TESTING'] = True
    CONFIG_DICT['FAULT_LOG_FILE_PATH'] = '/dev/null'
