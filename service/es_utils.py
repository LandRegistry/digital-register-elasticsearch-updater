import logging
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import bulk

from config import CONFIG_DICT


LOGGER = logging.getLogger(__name__)

ELASTICSEARCH_NODES = [CONFIG_DICT['ELASTICSEARCH_URI']]

elasticsearch_client = Elasticsearch(ELASTICSEARCH_NODES)
indices_client = IndicesClient(elasticsearch_client)


def ensure_mapping_exists(index_name, doc_type, mapping):
    if index_name not in elasticsearch_client.indices.status()['indices']:
        LOGGER.info(
            "Index '{}' not found in elasticsearch. Creating...".format(index_name)
        )

        elasticsearch_client.index(index=index_name, doc_type=doc_type, body={})
    else:
        LOGGER.info("Index '{}' with doc type '{}' already exists".format(index_name, doc_type))

    LOGGER.info("Ensuring mapping exists for index '{}', doc type '{}'".format(
        index_name, doc_type
    ))

    indices_client.put_mapping(
        index=index_name, doc_type=doc_type, body=mapping,
    )


def execute_elasticsearch_actions(actions):
    return bulk(elasticsearch_client, actions)


def search(query_dict, index_name, doc_type):
    result = elasticsearch_client.search(
        index=index_name, doc_type=doc_type, body=query_dict
    )

    return result['hits']['hits']


def get_upsert_action(index_name, doc_type, document, id):
    return {
        'doc_as_upsert': True,
        '_op_type': 'update',
        '_index': index_name,
        '_type': doc_type,
        '_id': id,
        'doc': document,
    }


def get_delete_action(index_name, doc_type, id):
    return {
        '_op_type': 'delete',
        '_index': index_name,
        '_type': doc_type,
        '_id': id,
    }


def get_cluster_info():
    return elasticsearch_client.info()
