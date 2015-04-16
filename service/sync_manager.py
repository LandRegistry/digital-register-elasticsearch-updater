import logging
import threading

from apscheduler.schedulers.background import BackgroundScheduler
from flask import json

from config import CONFIG_DICT
from service import synchroniser
from service import es_utils
from service.updaters.property_by_address_updater_v1 import PropertyByAddressUpdaterV1
from service.updaters.property_by_postcode_updater_v1 import PropertyByPostcodeUpdaterV1
from service.updaters.property_by_postcode_updater_v2 import PropertyByPostcodeUpdaterV2


LOGGER = logging.getLogger(__name__)
UPDATER_STATUS_BUSY = "busy"
UPDATER_STATUS_IDLE = "idle"
updater_status_lock = threading.RLock()

indexes = None
updater_statuses = None
index_updaters = None
polling_interval_in_secs = CONFIG_DICT['POLLING_INTERVAL_SECS']

scheduler = BackgroundScheduler()


# schedule synchronisation/update events
def start():
    global indexes
    global updater_statuses
    global index_updaters
    
    LOGGER.info("Scheduling index synchronisation to take place every {} seconds".format(
        polling_interval_in_secs
    ))
    
    indexes = _get_index_data_from_config()
    updater_statuses = {updater_id: UPDATER_STATUS_IDLE for updater_id in indexes}
    index_updaters = [_get_updater(updater_id) for updater_id in indexes]

    for updater in index_updaters:
        _ensure_mapping_exists(updater)
        _initialise_index_updater(updater)

    _schedule_data_synchronisation()
    
    LOGGER.info('Index synchronisation scheduled')


def _schedule_data_synchronisation():
    scheduler.add_job(
        _synchronise_es_indexes_with_source,
        'interval',
        seconds=polling_interval_in_secs
    )

    scheduler.start()


def _get_updater(updater_id):
    """Gets and instance of the right elasticsearch updater, based on the ID"""
    
    updaters_creators = {
        'property-by-postcode-v1-updater':
            lambda : PropertyByPostcodeUpdaterV1(),
        'property-by-postcode-v2-updater':
            lambda : PropertyByPostcodeUpdaterV2(),
        'property-by-address-v1-updater':
            lambda : PropertyByAddressUpdaterV1()
    }

    creator = updaters_creators.get(updater_id)

    if creator is not None:
        updater = creator()
        updater.id = updater_id
        return updater
    else:
        error_msg = "Failed to create index updater - unrecognised updater ID: '{}'"
        raise Exception(error_msg.format(updater_id))


def _initialise_index_updater(index_updater):
    LOGGER.info("Initialising index updater '{}'".format(index_updater.id))
    index_updater.initialise(
        _get_index_name(index_updater), _get_doc_type(index_updater)
    )
    LOGGER.info("Initialised index updater '{}'".format(index_updater.id))


# method to be scheduled - synchronisation of all indexes
def _synchronise_es_indexes_with_source():
    LOGGER.info('Starting index synchronisation')
    
    try:
        for index_updater in index_updaters:
            if not _is_index_updater_busy(index_updater):
                _trigger_index_synchronisation(index_updater)
            else:
                LOGGER.info("Updater '{}' is busy - skipping".format(index_updater.id))
                
        LOGGER.info('Index synchronisation started')
    except Exception as e:
        LOGGER.error('An error occurred when starting index synchronisation', e)
    
            
def _trigger_index_synchronisation(index_updater):
    LOGGER.info("Starting data synchronisation using updater '{}'".format(index_updater.id))

    t = threading.Thread(
        target=_synchronise_index_with_source,
        args = (index_updater,)
    )
    t.daemon = True
    t.start()

    
def _synchronise_index_with_source(index_updater):
    _update_index_updater_status(index_updater, busy=True)
    
    try:
        synchroniser.synchronise_index_with_source(
            index_updater, _get_index_name(index_updater), _get_doc_type(index_updater)
        )
    except Exception as e:
        LOGGER.error(
            "An error occurred when updating elasticsearch using updater '{}'".format(
                index_updater.id
            ),
            e
        )
    finally:
        _update_index_updater_status(index_updater, busy=False)


def _is_index_updater_busy(index_updater):
    with updater_status_lock:
        return updater_statuses[index_updater.id] == UPDATER_STATUS_BUSY


def _update_index_updater_status(index_updater, busy):
    with updater_status_lock:
        if busy:
            updater_statuses[index_updater.id] = UPDATER_STATUS_BUSY
        else:
            updater_statuses[index_updater.id] = UPDATER_STATUS_IDLE


def _ensure_mapping_exists(index_updater):
    LOGGER.info("Checking if the schema is prepared for index updater '{}'".format(
        index_updater.id
    ))

    index_name = _get_index_name(index_updater)
    doc_type = _get_doc_type(index_updater)
    mapping = index_updater.get_mapping()

    es_utils.ensure_mapping_exists(index_name, doc_type, mapping)


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


def _get_index_name(index_updater):
    return indexes[index_updater.id]['index_name']


def _get_doc_type(index_updater):
    return indexes[index_updater.id]['doc_type']
