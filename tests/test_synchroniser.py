from collections import namedtuple
from datetime import datetime
from mock import MagicMock, call
import mock
import pytest
from config import CONFIG_DICT
from freezegun import freeze_time
from service import synchroniser

MockTitleRegisterData = namedtuple(
    "TitleRegisterData", ['title_number', 'register_data', 'last_modified', 'is_deleted']
)

FROZEN_NOW_STRING = '2015-04-30 12:34:56'
FROZEN_NOW = datetime(2015, 4, 30, 12, 34, 56)


class TestSynchroniser:

    @freeze_time(FROZEN_NOW_STRING)
    @mock.patch('service.es_utils.execute_elasticsearch_actions', return_value=(1, []))
    def test_synchronise_index_with_source_uses_updater_correctly(self, mock_execute_es_actions):
        last_title_number = 'TTL123'
        last_modified_datetime = datetime(2015, 5, 10, 11, 12, 13)
        title1 = MockTitleRegisterData('first_title', {'register': 'data'}, datetime.now(), False)
        title2 = MockTitleRegisterData(
            last_title_number, {'register': 'data'}, last_modified_datetime, False
        )
        data_page = [title1, title2]
        elasticsearch_action_1 = [{'update': 'action1'}]
        elasticsearch_action_2 = [{'update': 'action2'}]

        mock_updater = MagicMock()

        mock_updater.get_next_source_data_page.return_value = data_page
        mock_updater.prepare_elasticsearch_actions.side_effect = [
            [elasticsearch_action_1], [elasticsearch_action_2]
        ]

        synchroniser.synchronise_index_with_source(mock_updater)

        mock_updater.get_next_source_data_page.assert_called_once_with(CONFIG_DICT['PAGE_SIZE'])
        assert mock_updater.prepare_elasticsearch_actions.mock_calls == [
            call(title1), call(title2)
        ]

        assert mock_updater.last_title_modification_date == last_modified_datetime
        assert mock_updater.last_updated_title_number == last_title_number
        assert mock_updater.last_successful_sync_time == FROZEN_NOW

        mock_execute_es_actions.assert_called_once_with(
            [elasticsearch_action_1, elasticsearch_action_2]
        )

    @mock.patch('service.es_status_loader.load_index_updater_status')
    def test_synchronise_index_calls_status_loader_when_updater_has_no_status(
            self, mock_load_index_updater_status):

        mock_updater = MagicMock()
        mock_updater.last_title_modification_date = None
        synchroniser.synchronise_index_with_source(mock_updater)
        mock_load_index_updater_status.assert_called_once_with(mock_updater)

    @mock.patch('service.es_status_loader.load_index_updater_status')
    def test_synchronise_index_does_not_call_status_loader_when_updater_has_status(
            self, mock_load_index_updater_status):

        mock_updater = MagicMock()
        mock_updater.last_title_modification_date = datetime.now()
        synchroniser.synchronise_index_with_source(mock_updater)
        assert mock_load_index_updater_status.mock_calls == []

    def test_synchronise_index_aborts_when_status_loader_fails(self):
        with mock.patch(
                'service.es_status_loader.load_index_updater_status',
                side_effect=Exception('Intentionally raised test exception')
        ):
            mock_updater = MagicMock()
            mock_updater.last_title_modification_date = None

            synchroniser.synchronise_index_with_source(mock_updater)

            # Check if the updater hasn't been used
            assert mock_updater.get_next_source_data_page.mock_calls == []

    @freeze_time(FROZEN_NOW_STRING)
    @mock.patch('service.es_utils.execute_elasticsearch_actions', return_value=(1, []))
    def test_synchronise_index_with_source_repeats_until_no_source_data_present(
            self, mock_execute_es_actions):

        synchroniser.page_size = 2
        last_title_number = 'TTL3'
        last_modified_datetime = datetime(2015, 5, 10, 11, 12, 13)

        title1 = MockTitleRegisterData('TTL1', {'register': 'data1'}, datetime.now(), False)
        title2 = MockTitleRegisterData('TTL2', {'register': 'data2'}, datetime.now(), False)
        title3 = MockTitleRegisterData(
            last_title_number, {'register': 'data3'}, last_modified_datetime, False
        )

        mock_updater = MagicMock()

        mock_updater.get_next_source_data_page.side_effect = [[title1, title2], [title3]]
        mock_updater.prepare_elasticsearch_actions.return_value = [{'update': 'action1'}]

        synchroniser.synchronise_index_with_source(mock_updater)

        assert len(mock_updater.get_next_source_data_page.mock_calls) == 2
        assert mock_updater.prepare_elasticsearch_actions.mock_calls == [
            call(title1), call(title2), call(title3)
        ]
        assert mock_updater.last_title_modification_date == last_modified_datetime
        assert mock_updater.last_updated_title_number == last_title_number
        assert mock_updater.last_successful_sync_time == FROZEN_NOW
        assert len(mock_execute_es_actions.mock_calls) == 2

    @freeze_time(FROZEN_NOW_STRING)
    @mock.patch('service.es_utils.execute_elasticsearch_actions', return_value=(1, []))
    def test_synchronise_index_stops_when_updater_fails(self, mock_execute_es_actions):
        synchroniser.page_size = 1
        last_modified_datetime = datetime(2015, 5, 10, 11, 12, 13)
        last_updated_title_number = 'TTL0'

        title1 = MockTitleRegisterData('TTL1', {'register': 'data1'}, datetime.now(), False)
        title2 = MockTitleRegisterData('TTL2', {'register': 'data2'}, datetime.now(), False)

        mock_updater = MagicMock()

        mock_updater.get_next_source_data_page.side_effect = [[title1], [title2]]
        mock_updater.prepare_elasticsearch_actions.side_effect = Exception(
            'Intentionally raised test exception'
        )
        mock_updater.last_title_modification_date = last_modified_datetime
        mock_updater.last_updated_title_number = last_updated_title_number
        mock_updater.last_successful_sync_time = None
        mock_updater.last_unsuccessful_sync_time = None

        synchroniser.synchronise_index_with_source(mock_updater)

        assert len(mock_updater.get_next_source_data_page.mock_calls) == 1
        assert len(mock_updater.prepare_elasticsearch_actions.mock_calls) == 1

        # Ensure that the updater status hasn't been changed
        assert mock_updater.last_title_modification_date == last_modified_datetime
        assert mock_updater.last_updated_title_number == last_updated_title_number
        assert not mock_updater.last_successful_sync_time
        assert mock_updater.last_unsuccessful_sync_time == FROZEN_NOW

        assert mock_execute_es_actions.mock_calls == []
