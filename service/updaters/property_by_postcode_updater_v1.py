import logging
import re
from service import es_utils
from service.database import page_reader
from service.updaters.base import AbstractIndexUpdater


LOGGER = logging.getLogger(__name__)
POSTCODE_REGEX = r'[A-Z]{1,2}[0-9R][0-9A-Z]? [0-9][A-Z]{2}'


class PropertyByPostcodeUpdaterV1(AbstractIndexUpdater):
    """elasticsearch data updater for property_by_postcode doc type in version 1"""

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
                'postcode': {'type': 'string', 'index': 'not_analyzed'},
                'entry_datetime': {
                    'type': 'date',
                    'format': 'date_time_no_millis',
                    'index': 'no'},
                }
        }

    def _prepare_delete_actions(self, title):
        def get_action(postcode):
            id = self._get_document_id(title.title_number, postcode)
            return es_utils.get_delete_action(self.index_name, self.doc_type, id)

        return [get_action(postcode) for postcode in self._get_postcodes(title.register_data)]

    def _prepare_upsert_actions(self, title):
        def get_action(postcode):
            id = self._get_document_id(title.title_number, postcode)

            document = {
                'title_number': title.title_number,
                'entry_datetime': title.last_modified.strftime('%y-%m-%dT%H:%M:%S+00'),
                'postcode': postcode,
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
