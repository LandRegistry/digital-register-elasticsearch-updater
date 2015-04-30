from collections import namedtuple
from datetime import datetime
from mock import MagicMock, call
import mock
from config import CONFIG_DICT
from service import synchroniser

MockTitleRegisterData = namedtuple(
    "TitleRegisterData", ['title_number', 'register_data', 'last_modified', 'is_deleted']
)


class TestSynchroniser:

    @mock.patch('service.es_utils.execute_elasticsearch_actions', return_value=(1, []))
    def test_synchronise_index_with_source_uses_updater_correctly(self, mock_execute_es_actions):
        title = MockTitleRegisterData('TTL123', {'register': 'data'}, datetime.now(), False)
        data_page = [title]
        elasticsearch_actions = [{'update': 'action'}]

        mock_updater = MagicMock()

        mock_updater.get_next_source_data_page.return_value = data_page
        mock_updater.prepare_elasticsearch_actions.return_value = elasticsearch_actions

        synchroniser.synchronise_index_with_source(mock_updater, 'index_name1', 'doc_type1')

        mock_updater.get_next_source_data_page.assert_called_once_with(CONFIG_DICT['PAGE_SIZE'])
        mock_updater.prepare_elasticsearch_actions.assert_called_once_with(title)
        mock_updater.update_status.assert_called_once_with(data_page)

        mock_execute_es_actions.assert_called_once_with(elasticsearch_actions)

    @mock.patch('service.es_utils.execute_elasticsearch_actions', return_value=(1, []))
    def test_synchronise_index_with_source_repeats_until_no_source_data_present(
            self, mock_execute_es_actions):

        synchroniser.page_size = 2

        title1 = MockTitleRegisterData('TTL1', {'register': 'data1'}, datetime.now(), False)
        title2 = MockTitleRegisterData('TTL2', {'register': 'data2'}, datetime.now(), False)
        title3 = MockTitleRegisterData('TTL3', {'register': 'data3'}, datetime.now(), False)

        mock_updater = MagicMock()

        mock_updater.get_next_source_data_page.side_effect = [[title1, title2], [title3]]
        mock_updater.prepare_elasticsearch_actions.return_value = [{'update': 'action1'}]

        synchroniser.synchronise_index_with_source(mock_updater, 'index_name1', 'doc_type1')

        assert len(mock_updater.get_next_source_data_page.mock_calls) == 2

        assert mock_updater.prepare_elasticsearch_actions.mock_calls == [
            call(title1), call(title2), call(title3)
        ]

        assert mock_updater.update_status.mock_calls == [call([title1, title2]), call([title3])]
        assert len(mock_execute_es_actions.mock_calls) == 2
