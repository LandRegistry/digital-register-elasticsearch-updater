from abc import ABCMeta, abstractmethod

# TODO: descriptions
import logging

LOGGER = logging.getLogger(__name__)

class AbstractIndexUpdater():
    """Base class for elasticsearch data updaters - enforces the right interface"""
    
    __metaclass__ = ABCMeta

    _id = None

    @property
    def id(self):
        return self._id
    
    @id.setter
    def id(self, value):
        self._id = value

    @abstractmethod
    def initialise(self, index_name, doc_type):
        pass

    @abstractmethod
    def get_next_source_data_page(self, page_size):
        return []

    @abstractmethod
    def prepare_elasticsearch_actions(self, title):
        return []

    @abstractmethod
    def update_status(self, data_page):
        pass

    @abstractmethod
    def get_mapping(self):
        pass
