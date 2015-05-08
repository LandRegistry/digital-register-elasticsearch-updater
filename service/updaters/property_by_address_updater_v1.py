# TODO: think about the date rounding issues
# TODO: date with milliseconds
from datetime import datetime
import logging
import re
from service import date_utils
from service import es_status_loader
from service.database.page_reader import get_next_data_page
from service import es_utils


from service.updaters.base import AbstractIndexUpdater

LOGGER = logging.getLogger(__name__)


class PropertyByAddressUpdaterV1(AbstractIndexUpdater):
    """elasticsearch data updater for property_by_address doc type in version 1"""

    last_title_modification_date = None
    last_updated_title_number = None
    last_sync_time = None
    index_name = None
    doc_type = None

    def initialise(self, index_name, doc_type):
        self.index_name = index_name
        self.doc_type = doc_type
        self._load_status()

    def get_next_source_data_page(self, page_size):
        if self.last_title_modification_date is None:
            LOGGER.warn("Unknown index update status for index '{}', doc type '{}'".format(
                self.index_name, self.doc_type
            ))
            self._load_status()

        self.last_sync_time = datetime.now()

        source_data_page = get_next_data_page(
            self.last_updated_title_number,
            self.last_title_modification_date,
            page_size,
        )

        return source_data_page

    def prepare_elasticsearch_actions(self, title):
        if title.is_deleted:
            return self._prepare_delete_actions(title)
        else:
            return self._prepare_upsert_actions(title)

    def update_status(self, data_page):
        latest_title = data_page[-1]
        self.last_title_modification_date = latest_title.last_modified
        self.last_updated_title_number = latest_title.title_number
        LOGGER.info("Status update: last modified title: '{}'".format(latest_title.last_modified))

    def get_mapping(self):
        return {
            'properties': {
                'title_number': {'type': 'string', 'index': 'no'},
                'address_string': {'type': 'string', 'index': 'analyzed'},
                'entry_datetime': {'type': 'date',
                                   'format': 'date_time',
                                   'index': 'no'},
            }
        }

    def _load_status(self):
        index_update_status = es_status_loader.load_index_update_status(
            self.index_name, self.doc_type
        )

        self.last_title_modification_date = index_update_status['last_modification_date']
        self.last_updated_title_number = index_update_status['last_updated_title_number']

    def _prepare_delete_actions(self, title):
        id = self._get_document_id(title.title_number, self._get_address_string(title))
        return [es_utils.get_delete_action(self.index_name, self.doc_type, id)]

    def _prepare_upsert_actions(self, title):
        address_string = self._get_address_string(title)

        id = self._get_document_id(title.title_number, address_string)

        document = {
            'title_number': title.title_number,
            'entry_datetime': date_utils.format_date_with_millis(title.last_modified),
            'address_string': address_string,
        }

        return [es_utils.get_upsert_action(self.index_name, self.doc_type, document, id)]

    def _get_document_id(self, title_number, address_string):
        id = '{}-{}'.format(title_number, address_string.upper())
        normalised_id = re.sub('\\s+', '_', id)
        return normalised_id

    def _get_address_string(self, title):
        address_string = title.register_data['address']['address_string']
        normalised_address_string = re.sub('[,()]', '', address_string)
        return normalised_address_string
