from datetime import datetime, timezone
import mock
import pytest
from service import es_status_loader


class TestEsStatusLoader:

    @mock.patch('service.es_utils.search', return_value=[])
    def test_load_index_update_status_calls_es_utils(self, mock_search):
        index_name = 'index_name_1'
        doc_type = 'doc_type_1'

        es_status_loader._retrieve_index_updater_status(index_name, doc_type)

        expected_search_query = {
            'filter': {
                'match_all': {
                }
            },
            'sort': [
                {
                    'entry_datetime': {
                        'order': 'desc'
                    }
                }
            ],
            'size': 1
        }

        mock_search.assert_called_once_with(expected_search_query, index_name, doc_type)

    def test_load_index_update_status_returns_last_title_info(self):
        entry_datetime_string = '2015-04-20T11:23:34.000+00'
        expected_datetime = datetime(2015, 4, 20, 11, 23, 34, 0, timezone.utc)
        title_number = 'ABC123'

        es_search_result = [{
            '_source': {
                'entry_datetime': entry_datetime_string,
                'title_number': title_number
            }
        }]

        with mock.patch('service.es_utils.search', return_value=es_search_result):
            result = es_status_loader._retrieve_index_updater_status('index_name', 'doc_type')

            assert result == {
                'last_modification_date': expected_datetime,
                'last_updated_title_number': title_number,
            }

    def test_load_index_update_status_converts_the_date_to_contain_four_digit_long_year(self):
        entry_datetime_string = '15-04-20T11:23:34.000+00'
        expected_datetime = datetime(2015, 4, 20, 11, 23, 34, 0, timezone.utc)
        title_number = 'ABC123'

        es_search_result = [{
            '_source': {
                'entry_datetime': entry_datetime_string,
                'title_number': title_number
            }
        }]

        with mock.patch('service.es_utils.search', return_value=es_search_result):
            result = es_status_loader._retrieve_index_updater_status('index_name', 'doc_type')

            assert result['last_modification_date'] == expected_datetime

    @mock.patch('service.es_utils.search', return_value=None)
    def test_load_index_update_status_returns_defaults_when_no_title(self, mock_search):
        result = es_status_loader._retrieve_index_updater_status('index_name', 'doc_type')

        assert result == {
            'last_modification_date': datetime.min,
            'last_updated_title_number': '',
        }

    @mock.patch('service.es_utils.search', return_value=[{'_source': {}}])
    def test_load_index_update_status_returns_defaults_when_empty_record(self, mock_search):
        result = es_status_loader._retrieve_index_updater_status('index_name', 'doc_type')

        assert result == {
            'last_modification_date': datetime.min,
            'last_updated_title_number': '',
        }
