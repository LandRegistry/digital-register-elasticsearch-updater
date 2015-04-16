import mock
from service import es_utils

# TODO: partly to be replaced by proper integration tests

class TestEsUtils:

    @mock.patch(
        'service.es_utils.elasticsearch_client.indices.status',
        return_value={'indices': []}
    )
    @mock.patch('service.es_utils.elasticsearch_client.index')
    @mock.patch('service.es_utils.indices_client.put_mapping')
    def test_ensure_mapping_exists_creates_index_when_one_not_present(
            self, mock_put_mapping, mock_index, mock_status):

        index_name = 'index_name'
        doc_type = 'doc_type'
        mapping = {'mapping': 'properties'}

        es_utils.ensure_mapping_exists(index_name, doc_type, mapping)

        mock_index.assert_called_once_with(index=index_name, doc_type=doc_type, body={})
        mock_put_mapping.assert_called_once_with(
            index=index_name, doc_type=doc_type, body=mapping
        )

    @mock.patch(
        'service.es_utils.elasticsearch_client.indices.status',
        return_value={'indices': ['index_name']})
    @mock.patch('service.es_utils.elasticsearch_client.index')
    @mock.patch('service.es_utils.indices_client.put_mapping')
    def test_ensure_mapping_exists_does_not_create_index_when_present(
            self, mock_put_mapping, mock_index, mock_status):

        index_name = 'index_name'
        doc_type = 'doc_type'
        mapping = {'mapping': 'properties'}

        es_utils.ensure_mapping_exists(index_name, doc_type, mapping)

        assert mock_index.mock_calls == []
        mock_put_mapping.assert_called_once_with(
            index=index_name, doc_type=doc_type, body=mapping
        )

    @mock.patch('service.es_utils.bulk')
    def test_execute_elasticsearch_actions_executes_all_given_actions(self, mock_bulk):
        actions = [{'action1': '1', 'action2': '2'}]
        es_utils.execute_elasticsearch_actions(actions)

        mock_bulk.assert_called_once_with(es_utils.elasticsearch_client, actions)

    def test_execute_elasticsearch_actions_returns_execution_result(self):
        expected_result = (123, ['error1'])

        with mock.patch('service.es_utils.bulk', return_value = expected_result):
            result = es_utils.execute_elasticsearch_actions([])

            assert result == expected_result

    @mock.patch('service.es_utils.elasticsearch_client.search')
    def test_search_executes_given_query(self, mock_search):
        query_dict = {'query': 'dict'}
        index_name = 'index_name'
        doc_type = 'doc_type'

        es_utils.search(query_dict, index_name, doc_type)

        mock_search.assert_called_once_with(index=index_name, doc_type=doc_type, body=query_dict)

    def test_search_returns_the_hits_from_elasticsearch_result(self):
        hits = [{'_source': {'some': 'data'}}]
        search_result = {'hits': {'hits': hits}}

        with mock.patch(
                'service.es_utils.elasticsearch_client.search',
                return_value = search_result
        ):
            result = es_utils.search({'query': 'dict'}, 'index_name', 'doc_type')

            assert result == hits

    def test_get_upsert_action_returns_action_with_the_right_content(self):
        result = es_utils.get_upsert_action('index_name1', 'doc_type1', {'document': 'body1'}, 'id1')

        assert result == {
            'doc_as_upsert': True,
            '_op_type': 'update',
            '_index': 'index_name1',
            '_type': 'doc_type1',
            '_id': 'id1',
            'doc': {'document': 'body1'},
        }

    def test_get_delete_action_returns_action_with_the_right_content(self):
        result = es_utils.get_delete_action('index_name1', 'doc_type1', 'id1')

        assert result == {
            '_op_type': 'delete',
            '_index': 'index_name1',
            '_type': 'doc_type1',
            '_id': 'id1',
        }
