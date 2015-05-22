#!/bin/sh
export SQLALCHEMY_DATABASE_URI=postgresql+pg8000://postgres:password@172.16.42.43:5432/register_data
export INDEX_CONFIG_FILE_PATH=index_updaters.json
export LOGGING_CONFIG_FILE_PATH=logging_config.json
export ELASTICSEARCH_URI=http://localhost:9200
export PAGE_SIZE=100
export POLLING_INTERVAL_SECS=2
export PYTHONPATH=.
export FAULT_LOG_FILE_PATH='/var/log/applications/digital-register-elasticsearch-updater-fault.log'
