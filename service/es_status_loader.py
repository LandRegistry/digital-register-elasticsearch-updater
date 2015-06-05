from datetime import datetime
import logging
import re
from service import date_utils, es_utils

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


def load_index_updater_status(index_updater):
    LOGGER.info("Loading update status for '{}'".format(index_updater.id))
    index_update_status = _retrieve_index_updater_status(
        index_updater.index_name, index_updater.doc_type
    )

    index_updater.last_title_modification_date = index_update_status['last_modification_date']
    index_updater.last_updated_title_number = index_update_status['last_updated_title_number']
    LOGGER.info("Loaded update status for '{}'".format(index_updater.id))


def _retrieve_index_updater_status(index_name, doc_type):
    _log_status_load_start(index_name, doc_type)

    try:
        latest_title_result = es_utils.search(SEARCH_QUERY, index_name, doc_type)
        latest_title = latest_title_result[0] if latest_title_result else None

        # default values
        last_modification_date = datetime.min
        last_updated_title_number = ''

        if latest_title:
            latest_title_data = latest_title.get('_source', {})
            if 'entry_datetime' in latest_title_data:
                last_modification_string = _fix_datetime(latest_title_data['entry_datetime'])
                last_modification_date = date_utils.string_to_date(last_modification_string)
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
    """Get date with 4 digit year, milliseconds and 4 digit timezone.
    >>> _fix_datetime('15-05-26T18:09:51+00')
    '2015-05-26T18:09:51.000+0000'
    >>> _fix_datetime('2015-05-26T18:09:51.000+0000')
    '2015-05-26T18:09:51.000+0000'
    """
    year_part, _, non_year_parts = datetime_string.partition('-')
    non_year_datetime_and_ms_parts, _, tz_part = non_year_parts.partition('+')
    non_year_datetime_part, _, ms_part = non_year_datetime_and_ms_parts.partition('.')

    year_part = '20{}'.format(year_part) if len(year_part) == 2 else year_part
    ms_part = ms_part[:3].rjust(3, '0')
    tz_part = tz_part[:4].rjust(4, '0')
    return ''.join([year_part, '-', non_year_datetime_part, '.', ms_part, '+', tz_part])
