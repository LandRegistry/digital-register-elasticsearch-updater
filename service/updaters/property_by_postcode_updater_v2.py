from datetime import datetime
import logging
import re
from service import date_utils
from service import es_utils
from service.database.page_reader import get_next_data_page
from service.updaters.base import AbstractIndexUpdater
from service import es_status_loader


LOGGER = logging.getLogger(__name__)
POSTCODE_REGEX = r'[A-Z]{1,2}[0-9R][0-9A-Z]? [0-9][A-Z]{2}'


class PropertyByPostcodeUpdaterV2(AbstractIndexUpdater):
    """elasticsearch data updater for property_by_postcode doc type in version 2"""

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

    def get_mapping(self):
        return {
            'properties': {
                'title_number': {'type': 'string', 'index': 'no'},
                'postcode': {'type': 'string', 'index': 'no'},
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
        def get_action(postcode):
            normalised_postcode = self._normalise_postcode(postcode)
            id = self._get_document_id(title.title_number, normalised_postcode)
            return es_utils.get_delete_action(self.index_name, self.doc_type, id)

        return [get_action(postcode) for postcode in self._get_postcodes(title.register_data)]

    def _prepare_upsert_actions(self, title):
        def get_action(postcode):
            normalised_postcode = self._normalise_postcode(postcode)
            id = self._get_document_id(title.title_number, normalised_postcode)

            document = {
                'title_number': title.title_number,
                'entry_datetime': date_utils.format_date_with_millis(title.last_modified),
                'postcode': normalised_postcode,
            }

            return es_utils.get_upsert_action(self.index_name, self.doc_type, document, id)

        return [get_action(postcode) for postcode in self._get_postcodes(title.register_data)]

    def _get_document_id(self, title_number, postcode):
        return '{}-{}'.format(title_number, postcode.upper())

    def _get_postcodes(self, title_dict):
        address_dict = title_dict['address']
        postcode = address_dict.get('postcode')
        if postcode:
            return [postcode]

        address_str = address_dict.get('address_string', None)
        if address_str:
            postcodes = re.findall(POSTCODE_REGEX, address_str)
            return postcodes
        return None

    def _normalise_postcode(self, postcode):
        return re.sub('\\s+', '', postcode)
