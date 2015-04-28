from datetime import datetime
import logging
import re
from service import es_utils

LOGGER = logging.getLogger(__name__)

SEARCH_QUERY = {
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


def load_index_update_status(index_name, doc_type):
    _log_status_load_start(index_name, doc_type)

    try:
        latest_title_result = es_utils.search(SEARCH_QUERY, index_name, doc_type)
        latest_title = latest_title_result[0] if latest_title_result else None

        if latest_title is None:
            last_modification_date = datetime.min
            last_updated_title_number = ''
        else:
            latest_title_data = latest_title['_source']
            if 'entry_datetime' not in latest_title_data:
                last_modification_date = datetime.min
                last_updated_title_number = ''
            else:
                last_modification_date = _fix_datetime(latest_title_data['entry_datetime'])
                last_updated_title_number = latest_title_data['title_number']

        _log_status_load_completion(index_name, doc_type, last_modification_date)
        return _format_status(last_modification_date, last_updated_title_number)
    except Exception as e:
        raise Exception(
            "Failed to load index update status. Index name: '{}', doc type: '{}'".format(
                index_name,
                doc_type
            ),
            e,
        )


def _log_status_load_start(index_name, doc_type):
    msg_format = "Loading index update status for index '{}', doc type '{}'"
    LOGGER.info(msg_format.format(index_name, doc_type))


def _log_status_load_completion(index_name, doc_type, last_modification_date):
    msg_format = "Loaded index update status for index '{}', doc type '{}'. Last update time: {}"
    LOGGER.info(msg_format.format(index_name, doc_type, last_modification_date))


def _format_status(last_modification_date, last_updated_title_number):
    return {
        "last_modification_date": last_modification_date,
        "last_updated_title_number": last_updated_title_number
    }


def _fix_datetime(datetime_string):
    '''Some dates in elasticsearch have got two-digit-long year.
    This method converts them to contain 4-digit-long year.'''

    if re.match('^\\d{2}-.+', datetime_string):
        return '20{}'.format(datetime_string)
    else:
        return datetime_string
