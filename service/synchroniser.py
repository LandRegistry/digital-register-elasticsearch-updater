
import itertools
import logging
from config import CONFIG_DICT
from service import es_utils


LOGGER = logging.getLogger(__name__)

page_size = CONFIG_DICT['PAGE_SIZE']


def synchronise_index_with_source(index_updater, index_name, doc_type):
    LOGGER.info(
        "Synchronising index '{}' with source data, doc type '{}', using updater '{}'".format(
            index_name, doc_type, index_updater.id
        )
    )

    if _bring_index_up_to_date(index_updater):
        LOGGER.info("Updater '{}' - finished synchronising index '{}', doc type '{}'".format(
            index_updater.id, index_name, doc_type
        ))
    else:
        LOGGER.error("Updater '{}' - aborted synchronising index '{}', doc type '{}'".format(
            index_updater.id, index_name, doc_type
        ))


def _bring_index_up_to_date(index_updater):
    is_up_to_date_with_source = False

    while not is_up_to_date_with_source:
        data_page, errors = _populate_index_with_data_page(index_updater)

        if errors:
            LOGGER.error("Elasticsearch update resulted with errors: {}".format(errors))
            return False
        else:
            LOGGER.info("Updated elasticsearch with page of data. Updater: '{}'".format(
                index_updater.id
            ))

            if data_page:
                index_updater.update_status(data_page)
                LOGGER.info("Updated synchronisation status for updater '{}'".format(
                    index_updater.id
                ))

            if len(data_page) < page_size:
                LOGGER.info("Updater '{}' is up to date with source data store".format(
                    index_updater.id
                ))
                is_up_to_date_with_source = True

    return True


def _populate_index_with_data_page(index_updater):
    data_page = _retrieve_source_data_page(index_updater)

    if data_page:
        elasticsearch_actions = _prepare_elasticsearch_actions(data_page, index_updater)

        success_count, errors = es_utils.execute_elasticsearch_actions(elasticsearch_actions)
        return data_page, errors
    else:
        return [], []


def _retrieve_source_data_page(index_updater):
    LOGGER.info("Retrieving page of source data. Updater: '{}', page size: {}".format(
        index_updater.id, page_size
    ))

    data_page = index_updater.get_next_source_data_page(page_size)

    LOGGER.info("Retrieved {} title(s). Updater: '{}'".format(
        len(data_page), index_updater.id)
    )

    return data_page


def _prepare_elasticsearch_actions(data_page, index_updater):
    LOGGER.info("Preparing elasticsearch actions. Updater: '{}'".format(index_updater.id))

    elasticsearch_action_lists = [
        index_updater.prepare_elasticsearch_actions(title) for title in data_page
    ]

    elasticsearch_actions = list(itertools.chain.from_iterable(elasticsearch_action_lists))

    LOGGER.info("Updating elasticsearch with data page. Updater: '{}'".format(index_updater.id))
    return elasticsearch_actions
