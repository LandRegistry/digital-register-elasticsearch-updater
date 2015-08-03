from abc import ABCMeta, abstractmethod

# TODO: descriptions
from datetime import datetime
import logging

LOGGER = logging.getLogger(__name__)


class AbstractIndexUpdater():
    """Base class for elasticsearch data updaters - enforces the right interface"""

    __metaclass__ = ABCMeta

    _id = None                            # type: str
    _index_name = None                    # type: str
    _doc_type = None                      # type: str
    _last_successful_sync_time = None     # type: datetime
    _last_unsuccessful_sync_time = None   # type: datetime
    _last_title_modification_date = None  # type: datetime
    _last_updated_title_number = None     # type: str

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def doc_type(self):
        return self._doc_type

    @property
    def index_name(self):
        return self._index_name

    @property
    def last_successful_sync_time(self):
        return self._last_successful_sync_time

    @last_successful_sync_time.setter
    def last_successful_sync_time(self, value):
        self._last_successful_sync_time = value

    @property
    def last_unsuccessful_sync_time(self):
        return self._last_unsuccessful_sync_time

    @last_unsuccessful_sync_time.setter
    def last_unsuccessful_sync_time(self, value):
        self._last_unsuccessful_sync_time = value

    @property
    def last_title_modification_date(self):
        return self._last_title_modification_date

    @last_title_modification_date.setter
    def last_title_modification_date(self, value):
        self._last_title_modification_date = value

    @property
    def last_updated_title_number(self):
        return self._last_updated_title_number

    @last_updated_title_number.setter
    def last_updated_title_number(self, value):
        self._last_updated_title_number = value

    def __init__(self, index_name, doc_type):
        self._index_name = index_name
        self._doc_type = doc_type

    @abstractmethod
    def get_next_source_data_page(self, page_size):
        return []

    @abstractmethod
    def prepare_elasticsearch_actions(self, title):
        return []

    @abstractmethod
    def get_mapping(self):
        pass
