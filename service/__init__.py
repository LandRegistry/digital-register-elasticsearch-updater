from flask import Flask
from sqlalchemy import create_engine
from config import CONFIG_DICT
from service import logging_config

app = Flask(__name__)
app.config.update(CONFIG_DICT)
db = create_engine(CONFIG_DICT['SQLALCHEMY_DATABASE_URI'])
logging_config.setup_logging()
