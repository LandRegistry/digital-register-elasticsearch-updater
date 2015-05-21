from datetime import datetime
import itertools
import logging
from config import CONFIG_DICT
from service import es_status_loader
from service import es_utils


LOGGER = logging.getLogger(__name__)

page_size = CONFIG_DICT['PAGE_SIZE']


def synchronise_index_with_source(index_updater):
    LOGGER.info(
        "Synchronising index '{}' with source data, doc type '{}', using updater '{}'".format(
            index_updater.index_name, index_updater.doc_type, index_updater.id
        )
    )

    try:
        _bring_index_up_to_date(index_updater)

        LOGGER.info("Updater '{}' - finished synchronising index '{}', doc type '{}'".format(
            index_updater.id, index_updater.index_name, index_updater.doc_type
        ))
    except Exception as e:
        LOGGER.error("Updater '{}' - aborted synchronising index '{}', doc type '{}'".format(
            index_updater.id, index_updater.index_name, index_updater.doc_type
        ), exc_info=e)


def _bring_index_up_to_date(index_updater):
    sync_time = datetime.now()
    _ensure_status_loaded(index_updater)
    is_up_to_date_with_source = False

    try:
        while not is_up_to_date_with_source:
            sync_time = datetime.now()
            data_page, errors = _populate_index_with_data_page(index_updater)

            # TODO: set only if no errors - otherwise set last_successful_sync_time. To be done
            # when ES error handling is solved
            index_updater.last_successful_sync_time = sync_time

            # TODO: investigate what types of errors to handle -
            # I've found 'not found' on deletions and 'conflict' on insert/updates, but they were
            # only informational - the updates took place

            if data_page:
                LOGGER.info("Updated elasticsearch with page of data. Updater: '{}'".format(
                    index_updater.id
                ))
                _update_index_updater_status(index_updater, data_page)

            if len(data_page) < page_size:
                LOGGER.info("Updater '{}' is up to date with source data store".format(
                    index_updater.id
                ))
                is_up_to_date_with_source = True
    except Exception as e:
        index_updater.last_unsuccessful_sync_time = sync_time
        raise e


def _update_index_updater_status(index_updater, data_page):
    latest_title = data_page[-1]
    index_updater.last_title_modification_date = latest_title.last_modified
    index_updater.last_updated_title_number = latest_title.title_number
    LOGGER.info("Updated sync status for updater '{}'. Last modified date: '{}'".format(
        index_updater.id, latest_title.last_modified
    ))


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


def _ensure_status_loaded(index_updater):
    if index_updater.last_title_modification_date is None:
        error_msg = "Unknown index update status for index '{}', doc type '{}', updater '{}'"
        LOGGER.warn(error_msg.format(
            index_updater.index_name, index_updater.doc_type, index_updater.id
        ))
        es_status_loader.load_index_updater_status(index_updater)


def _prepare_elasticsearch_actions(data_page, index_updater):
    LOGGER.info("Preparing elasticsearch actions. Updater: '{}'".format(index_updater.id))

    elasticsearch_action_lists = [
        index_updater.prepare_elasticsearch_actions(title) for title in data_page
    ]

    elasticsearch_actions = list(itertools.chain.from_iterable(elasticsearch_action_lists))

    LOGGER.info("Updating elasticsearch with data page. Updater: '{}'".format(index_updater.id))
    return elasticsearch_actions
