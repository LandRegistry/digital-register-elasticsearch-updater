import logging
import re
from service import date_utils
from service import es_utils
from service.database import page_reader
from service.updaters.base import AbstractIndexUpdater

LOGGER = logging.getLogger(__name__)
POSTCODE_REGEX = r'[A-Z]{1,2}[0-9R][0-9A-Z]? [0-9][A-Z]{2}'


class PropertyByPostcodeUpdaterV3(AbstractIndexUpdater):
    """elasticsearch data updater for property_by_postcode doc type in version 3"""

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
                'house_number_or_first_number': {'type': 'integer', 'index': 'not_analyzed'},
                'address_string': {'type': 'string', 'index': 'not_analyzed'},
                'entry_datetime': {'type': 'date',
                                   'format': 'date_time',
                                   'index': 'no'},

            }
        }

    def _prepare_delete_actions(self, title):
        def get_action(postcode):
            normalised_postcode = self._normalise_postcode(postcode)
            id = self._get_document_id(title.title_number, normalised_postcode)
            return es_utils.get_delete_action(self.index_name, self.doc_type, id)

        return [get_action(postcode) for postcode in self._get_postcodes(title.register_data)]

    def _prepare_upsert_actions(self, title):
        def get_action(postcode):
            normalised_postcode = self._normalise_postcode(postcode)
            address = title.register_data['address']
            address_string = address['address_string']
            id = self._get_document_id(title.title_number, normalised_postcode)
            house_no = address.get('house_no', None)
            house_number_or_first_number = self._get_house_number_or_first_number(address_string,
                                                                                  house_no)

            document = {
                'title_number': title.title_number,
                'entry_datetime': date_utils.format_date_with_millis(title.last_modified),
                'postcode': normalised_postcode,
                'house_number_or_first_number': house_number_or_first_number,
                'address_string': self._normalise_address_string(address_string),
            }

            return es_utils.get_upsert_action(self.index_name, self.doc_type, document, id)

        return [get_action(postcode) for postcode in self._get_postcodes(title.register_data)]

    def _get_house_number_or_first_number(self, address_string, house_no):
        if house_no and house_no.isdigit():
            return int(house_no)
        else:
            return self._first_number_not_in_postcode(address_string)

    def _normalise_address_string(self, address_string):
        return address_string.lower() if address_string else None

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

    def _first_number_not_in_postcode(self, address_string):
        address_without_postcodes = re.sub(POSTCODE_REGEX, '', address_string)
        numbers = re.findall(r'\d+', address_without_postcodes)
        if numbers:
            return int(numbers[0])
        else:
            return None

    def _normalise_postcode(self, postcode):
        return re.sub('\\s+', '', postcode)
