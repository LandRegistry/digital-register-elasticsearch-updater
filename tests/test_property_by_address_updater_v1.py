from collections import namedtuple
from datetime import datetime
import mock
from service.updaters.property_by_address_updater_v1 import PropertyByAddressUpdaterV1

MockTitleRegisterData = namedtuple(
    "TitleRegisterData", ['title_number', 'register_data', 'last_modified', 'is_deleted']
)

INDEX_LOAD_RESULT = {
    'last_modification_date': datetime.now(),
    'last_updated_title_number': 'title123'
}


class TestPropertyByAddressUpdaterV1:

    def test_initialise_loads_state(self):
        load_result = {
            'last_modification_date': '2015-04-30 12:23:34',
            'last_updated_title_number': 'title123'
        }

        index_name = 'index_name1'
        doc_type = 'doc_type1'

        with mock.patch(
                'service.es_status_loader.load_index_update_status',
                return_value=load_result
        ) as mock_load:
            PropertyByAddressUpdaterV1().initialise(index_name, doc_type)

            mock_load.assert_called_once_with(index_name, doc_type)

    @mock.patch(
        'service.updaters.property_by_address_updater_v1.get_next_data_page', return_value=[])
    def test_get_next_source_data_page_calls_page_reader_with_right_args(self, mock_get_page):
        last_title_number = 'title123'
        last_modification_date = datetime.now()
        page_size = 123

        load_result = {
            'last_modification_date': last_modification_date,
            'last_updated_title_number': last_title_number
        }

        with mock.patch(
                'service.es_status_loader.load_index_update_status',
                return_value=load_result
        ):
            PropertyByAddressUpdaterV1().get_next_source_data_page(page_size)

            mock_get_page.assert_called_once_with(
                last_title_number, last_modification_date, page_size
            )

    def test_get_next_source_data_page_returns_result_from_page_reader(self):
        title1 = MockTitleRegisterData('TTL1', {'register': 'data1'}, datetime.now(), False)
        title2 = MockTitleRegisterData('TTL2', {'register': 'data2'}, datetime.now(), False)

        with mock.patch('service.updaters.property_by_address_updater_v1.get_next_data_page',
                        return_value=[title1, title2]):
            with mock.patch('service.es_status_loader.load_index_update_status',
                            return_value=INDEX_LOAD_RESULT):
                result = PropertyByAddressUpdaterV1().get_next_source_data_page(123)

        assert result == [title1, title2]

    @mock.patch('service.es_utils.get_delete_action', return_value={'delete': 'action1'})
    def test_prepare_elasticsearch_actions_returns_delete_action_when_title_deleted(
            self, mock_get_delete_action):

        index_name = 'index_name1'
        doc_type = 'doc_type1'
        register_data = {'address': {'address_string': 'address string 1'}}

        deleted_title = MockTitleRegisterData('TTL1', register_data, datetime.now(), True)
        title_id = 'TTL1-ADDRESS_STRING_1'

        with mock.patch('service.es_status_loader.load_index_update_status',
                        return_value=INDEX_LOAD_RESULT):
            updater = PropertyByAddressUpdaterV1()
            updater.initialise(index_name, doc_type)
            returned_actions = updater.prepare_elasticsearch_actions(deleted_title)

            mock_get_delete_action.assert_called_once_with(index_name, doc_type, title_id)

            assert returned_actions == [{'delete': 'action1'}]

    @mock.patch('service.es_utils.get_upsert_action', return_value={'upsert': 'action1'})
    def test_prepare_elasticsearch_actions_returns_upsert_action_when_title_open(
            self, mock_get_upsert_action):

        entry_datetime = datetime(2015, 4, 20, 12, 23, 34)
        index_name = 'index_name1'
        doc_type = 'doc_type1'
        register_data = {'address': {'address_string': 'address string 1'}}
        updated_title = MockTitleRegisterData('TTL1', register_data, entry_datetime, False)
        title_id = 'TTL1-ADDRESS_STRING_1'
        doc = {
            'title_number': 'TTL1',
            'entry_datetime': '2015-04-20T12:23:34.000+00',
            'address_string': 'address string 1'
        }

        with mock.patch('service.es_status_loader.load_index_update_status',
                        return_value=INDEX_LOAD_RESULT):
            updater = PropertyByAddressUpdaterV1()
            updater.initialise(index_name, doc_type)
            returned_actions = updater.prepare_elasticsearch_actions(updated_title)

            mock_get_upsert_action.assert_called_once_with(index_name, doc_type, doc, title_id)

            assert returned_actions == [{'upsert': 'action1'}]

    @mock.patch(
        'service.updaters.property_by_address_updater_v1.get_next_data_page', return_value=[])
    def test_update_status_affects_the_arguments_to_get_next_source_data_page(
            self, mock_get_next_page):

        last_title_date = datetime(2015, 4, 2)
        last_title_number = 'TTL2'
        page_size = 123

        title1 = MockTitleRegisterData('TTL1', {'register': 'data1'}, datetime(2015, 4, 1), False)
        title2 = MockTitleRegisterData(
            last_title_number, {'register': 'data2'}, last_title_date, False
        )

        data_page = [title1, title2]

        updater = PropertyByAddressUpdaterV1()

        with mock.patch('service.es_status_loader.load_index_update_status',
                        return_value=INDEX_LOAD_RESULT):
            updater.initialise('index_name1', 'doc_type1')

        updater.update_status(data_page)
        updater.get_next_source_data_page(page_size)

        # check if next data page is requested based on the last updated title
        mock_get_next_page.assert_called_once_with(last_title_number, last_title_date, page_size)

    def test_get_mapping_returns_correct_mapping(self):
        assert PropertyByAddressUpdaterV1().get_mapping() == {
            'properties': {
                'title_number': {'type': 'string', 'index': 'no'},
                'address_string': {'type': 'string', 'index': 'not_analyzed'},
                'entry_datetime': {'type': 'date',
                                   'format': 'date_time',
                                   'index': 'no'},
                }
        }
