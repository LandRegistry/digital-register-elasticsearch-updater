from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore
from flask import json                                             # type: ignore
import logging
import threading
from typing import Dict

from config import CONFIG_DICT
from service import synchroniser
from service import es_status_loader
from service import es_utils
from service.updaters.property_by_address_updater_v1 import PropertyByAddressUpdaterV1
from service.updaters.property_by_postcode_updater_v3 import PropertyByPostcodeUpdaterV3


LOGGER = logging.getLogger(__name__)
UPDATER_STATUS_BUSY = "busy"
UPDATER_STATUS_IDLE = "idle"
updater_status_lock = threading.RLock()

_updater_statuses = None  # type: Dict[str, str]
_index_updaters = None    # type: Dict[str, Dict[str, str]]
_polling_interval_in_secs = CONFIG_DICT['POLLING_INTERVAL_SECS']

scheduler = BackgroundScheduler()


# schedule synchronisation/update events
def start():
    global _updater_statuses
    global _index_updaters

    LOGGER.info("Scheduling index synchronisation to take place every {} seconds".format(
        _polling_interval_in_secs
    ))

    indexes = _get_index_data_from_config()
    _updater_statuses = {updater_id: UPDATER_STATUS_IDLE for updater_id in indexes}
    _index_updaters = [_get_updater(updater_id, indexes[updater_id]) for updater_id in indexes]

    for updater in _index_updaters:
        _prepare_index_updater_for_use(updater)

    _schedule_data_synchronisation()

    LOGGER.info('Index synchronisation scheduled')


def get_index_updaters():
    return _index_updaters


def is_index_updater_busy(index_updater):
    with updater_status_lock:
        return _updater_statuses[index_updater.id] == UPDATER_STATUS_BUSY


# method to be scheduled - synchronisation of all indexes
def synchronise_es_indexes_with_source():
    LOGGER.info('Starting index synchronisation')

    try:
        for index_updater in _index_updaters:
            if not is_index_updater_busy(index_updater):
                _trigger_index_synchronisation(index_updater)
            else:
                LOGGER.info("Updater '{}' is busy - skipping".format(index_updater.id))

        LOGGER.info('Index synchronisation started')
    except Exception as e:
        LOGGER.error('An error occurred when starting index synchronisation', exc_info=e)


def _prepare_index_updater_for_use(updater):
    _ensure_mapping_exists(updater)
    es_status_loader.load_index_updater_status(updater)


def _schedule_data_synchronisation():
    scheduler.add_job(
        synchronise_es_indexes_with_source,
        'interval',
        seconds=_polling_interval_in_secs
    )

    scheduler.start()


def _get_updater(updater_id, index_info):
    """Gets and instance of the right elasticsearch updater, based on the ID"""

    index_name = index_info['index_name']
    doc_type = index_info['doc_type']

    updater_creators = {
        'property-by-postcode-v3-updater':
            lambda: PropertyByPostcodeUpdaterV3(index_name, doc_type),
        'property-by-address-v1-updater':
            lambda: PropertyByAddressUpdaterV1(index_name, doc_type)
    }

    creator = updater_creators.get(updater_id)

    if creator is not None:
        LOGGER.info("Creating index updater '{}'".format(updater_id))
        updater = creator()
        updater.id = updater_id
        LOGGER.info("Created index updater '{}'".format(updater_id))
        return updater
    else:
        error_msg = "Failed to create index updater - unrecognised updater ID: '{}'"
        raise Exception(error_msg.format(updater_id))


def _trigger_index_synchronisation(index_updater):
    LOGGER.info("Starting data synchronisation using updater '{}'".format(index_updater.id))

    t = threading.Thread(
        target=_synchronise_index_with_source,
        args=(index_updater,)
    )
    t.daemon = True
    t.start()


def _synchronise_index_with_source(index_updater):
    _update_index_updater_status(index_updater, busy=True)

    try:
        synchroniser.synchronise_index_with_source(index_updater)
    except Exception as e:
        LOGGER.error(
            "An error occurred when updating elasticsearch using updater '{}'".format(
                index_updater.id
            ),
            exc_info=e
        )
    finally:
        _update_index_updater_status(index_updater, busy=False)


def _update_index_updater_status(index_updater, busy):
    with updater_status_lock:
        if busy:
            _updater_statuses[index_updater.id] = UPDATER_STATUS_BUSY
        else:
            _updater_statuses[index_updater.id] = UPDATER_STATUS_IDLE


def _ensure_mapping_exists(index_updater):
    LOGGER.info("Checking if the schema is prepared for index updater '{}'".format(
        index_updater.id
    ))

    es_utils.ensure_mapping_exists(
        index_updater.index_name, index_updater.doc_type, index_updater.get_mapping()
    )


def _get_index_data_from_config():
    try:
        LOGGER.info('Loading index updater configuration file')
        index_config_file_path = CONFIG_DICT['INDEX_CONFIG_FILE_PATH']

        with open(index_config_file_path, 'rt') as file:
            indexes_json = json.load(file)

            # TODO: validate the json
            LOGGER.info('Index updater configuration file loaded ({})'.format(
                index_config_file_path
            ))

            return indexes_json['index_updaters']
    except IOError as e:
        raise (Exception('Failed to load index updater configuration', e))
