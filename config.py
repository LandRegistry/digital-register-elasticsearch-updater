import os
from typing import Dict, Union

CONFIG_DICT = {
    'DEBUG': False,
    'LOGGING': True,
    'LOGGING_CONFIG_FILE_PATH': os.environ['LOGGING_CONFIG_FILE_PATH'],
    'LOGGING_LEVEL': os.environ['LOGGING_LEVEL'],
    'FAULT_LOG_FILE_PATH': os.environ['FAULT_LOG_FILE_PATH'],
    'SQLALCHEMY_DATABASE_URI': os.environ['SQLALCHEMY_DATABASE_URI'],
    'INDEX_CONFIG_FILE_PATH': os.environ['INDEX_CONFIG_FILE_PATH'],
    'ELASTICSEARCH_URI': os.environ['ELASTICSEARCH_URI'],
    'PAGE_SIZE': int(os.environ['PAGE_SIZE']),
    'POLLING_INTERVAL_SECS': int(os.environ['POLLING_INTERVAL_SECS']),
}  # type: Dict[str, Union[bool, str, int]]

settings = os.environ.get('SETTINGS')

if settings == 'dev':
    CONFIG_DICT['DEBUG'] = True
elif settings == 'test':
    CONFIG_DICT['LOGGING'] = False
    CONFIG_DICT['DEBUG'] = True
    CONFIG_DICT['TESTING'] = True
    CONFIG_DICT['FAULT_LOG_FILE_PATH'] = '/dev/null'
