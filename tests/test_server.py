from datetime import datetime
import json
import mock
from mock import call
from config import CONFIG_DICT
from service.server import app


class TestServer:

    @mock.patch('service.sync_manager.is_index_updater_busy', return_value=True)
    def test_status_calls_sync_maganger_for_data(self, mock_is_updater_busy):
        mock_index_updater_1 = mock.MagicMock()
        mock_index_updater_1.last_successful_sync_time = datetime.now()
        mock_index_updater_1.last_title_modification_date = datetime.now()
        mock_index_updater_1.last_sync_attempt_successful = True
        mock_index_updater_1.index_name = 'index1'
        mock_index_updater_1.doc_type = 'doctype1'
        mock_index_updater_1.id = 'id1'

        with mock.patch(
                'service.sync_manager.get_index_updaters', return_value=[mock_index_updater_1]
        ) as mock_get_index_updaters:
            app.test_client().get('/status')

            mock_get_index_updaters.assert_called_once_with()
            assert mock_is_updater_busy.mock_calls == [call(mock_index_updater_1)]

    @mock.patch('service.sync_manager.is_index_updater_busy', side_effect=[True, False])
    def test_status_returns_data_retrieved_from_sync_manager(self, mock_is_updater_busy):
        mock_index_updater_1 = mock.MagicMock()
        mock_index_updater_1.last_successful_sync_time = datetime(2015, 4, 20, 10, 11, 12)
        mock_index_updater_1.last_unsuccessful_sync_time = datetime(2015, 4, 20, 12, 13, 14)
        mock_index_updater_1.last_title_modification_date = datetime(2015, 4, 21, 10, 11, 12)
        mock_index_updater_1.last_updated_title_number = 'title1'
        mock_index_updater_1.last_sync_attempt_successful = True
        mock_index_updater_1.index_name = 'index1'
        mock_index_updater_1.doc_type = 'doctype1'
        mock_index_updater_1.id = 'id1'

        mock_index_updater_2 = mock.MagicMock()
        mock_index_updater_2.last_successful_sync_time = datetime(2015, 4, 22, 10, 11, 12)
        mock_index_updater_2.last_unsuccessful_sync_time = datetime(2015, 4, 22, 12, 13, 14)
        mock_index_updater_2.last_title_modification_date = datetime(2015, 4, 23, 10, 11, 12)
        mock_index_updater_2.last_updated_title_number = 'title2'
        mock_index_updater_2.last_sync_attempt_successful = False
        mock_index_updater_2.index_name = 'index2'
        mock_index_updater_2.doc_type = 'doctype2'
        mock_index_updater_2.id = 'id2'

        updaters = [mock_index_updater_1, mock_index_updater_2]

        with mock.patch(
                'service.sync_manager.get_index_updaters', return_value=updaters):
            response = app.test_client().get('/status')
            assert response.status_code == 200
            response_json = json.loads(response.data.decode())
            assert response_json == {
                "polling_interval": CONFIG_DICT['POLLING_INTERVAL_SECS'],
                "status": {
                    "id1": {
                        "last_successful_sync_time": "2015-04-20T10:11:12.000+0000",
                        "last_unsuccessful_sync_time": "2015-04-20T12:13:14.000+0000",
                        "is_busy": True,
                        "index_name": "index1",
                        "doc_type": "doctype1",
                        "last_title_modification_date": "2015-04-21T10:11:12.000+0000",
                        "last_title_number": "title1",
                    },
                    "id2": {
                        "last_successful_sync_time": "2015-04-22T10:11:12.000+0000",
                        "last_unsuccessful_sync_time": "2015-04-22T12:13:14.000+0000",
                        "is_busy": False,
                        "index_name": "index2",
                        "doc_type": "doctype2",
                        "last_title_modification_date": "2015-04-23T10:11:12.000+0000",
                        "last_title_number": "title2",
                    },
                }
            }

    def test_status_returns_500_response_when_sync_manager_raises_error(self):
        exception_to_raise = Exception('Intentionally raised test exception')

        with mock.patch(
                'service.sync_manager.get_index_updaters', side_effect=exception_to_raise):
            response = app.test_client().get('/status')
            assert response.status_code == 500
            assert response.data.decode() == '{"error": "Internal server error"}'

    @mock.patch('service.database.page_reader.get_next_data_page', return_value=None)
    @mock.patch('service.server.es_utils.get_cluster_info', return_value={'status': 200})
    def test_health_returns_200_response_when_data_stores_respond_properly(
            self, mock_get_cluster_info, mock_get_next_data_page):

        response = app.test_client().get('/health')
        assert response.status_code == 200
        assert response.data.decode() == '{"status": "ok"}'

        mock_get_cluster_info.assert_called_once_with()
        mock_get_next_data_page.assert_called_once_with('', '2100-01-01', 1)

    @mock.patch('service.database.page_reader.get_next_data_page',
                side_effect=Exception('Test PG exception'))
    @mock.patch('service.server.es_utils.get_cluster_info', return_value={'status': 200})
    def test_health_returns_500_response_when_pg_access_fails(
            self, mock_get_cluster_info, mock_get_next_data_page):

        response = app.test_client().get('/health')

        assert response.status_code == 500
        json_response = json.loads(response.data.decode())
        assert json_response == {
            'status': 'error',
            'errors': ['Problem talking to PostgreSQL: Test PG exception'],
        }

    @mock.patch('service.database.page_reader.get_next_data_page', return_value=None)
    @mock.patch('service.server.es_utils.get_cluster_info',
                side_effect=Exception('Test ES exception'))
    def test_health_returns_500_response_when_es_access_fails(
            self, mock_get_cluster_info, mock_get_next_data_page):

        response = app.test_client().get('/health')

        assert response.status_code == 500
        json_response = json.loads(response.data.decode())
        assert json_response == {
            'status': 'error',
            'errors': ['Problem talking to elasticsearch: Test ES exception'],
        }

    @mock.patch('service.database.page_reader.get_next_data_page',
                side_effect=Exception('Test PG exception'))
    @mock.patch('service.server.es_utils.get_cluster_info',
                side_effect=Exception('Test ES exception'))
    def test_health_returns_500_response_with_multiple_errors_when_both_data_stores_fail(
            self, mock_get_cluster_info, mock_get_next_data_page):

        response = app.test_client().get('/health')

        assert response.status_code == 500
        json_response = json.loads(response.data.decode())
        assert json_response == {
            'status': 'error',
            'errors': [
                'Problem talking to elasticsearch: Test ES exception',
                'Problem talking to PostgreSQL: Test PG exception',
            ],
        }
