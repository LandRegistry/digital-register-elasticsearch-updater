import logging
import re
from service import date_utils
from service.database.page_reader import get_next_data_page
from service import es_utils
from service.updaters.base import AbstractIndexUpdater
from service.database import page_reader

LOGGER = logging.getLogger(__name__)


class PropertyByAddressUpdaterV1(AbstractIndexUpdater):
    """elasticsearch data updater for property_by_address doc type in version 1"""

    def get_next_source_data_page(self, page_size):
        source_data_page = page_reader.get_next_data_page(
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
