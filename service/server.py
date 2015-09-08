import json
import logging
from flask import Response  # type: ignore

from config import CONFIG_DICT
from service import sync_manager, app, es_utils
from service.database import page_reader
from service.date_utils import format_date_with_millis


LOGGER = logging.getLogger(__name__)

INTERNAL_SERVER_ERROR_RESPONSE_BODY = json.dumps({'error': 'Internal server error'})
APPLICATION_JSON_TYPE = 'application/json'


@app.errorhandler(Exception)
def handleServerError(error):
    LOGGER.error(
        'An error occurred when processing a request',
        exc_info=error
    )
    return _json_response(
        INTERNAL_SERVER_ERROR_RESPONSE_BODY,
        status=500,
    )


# TODO: remove the root route when the monitoring tools can work without it
@app.route('/', methods=['GET'])
@app.route('/health', methods=['GET'])
def healthcheck():
    errors = _check_elasticsearch_connection() + _check_postgresql_connection()
    status, http_status = ('error', 500) if errors else ('ok', 200)

    response_body = {'status': status}
    if errors:
        response_body['errors'] = errors

    return Response(
        json.dumps(response_body),
        status=http_status,
        mimetype=APPLICATION_JSON_TYPE,
    )


@app.route('/status', methods=['GET'])
def status():
    status_info = _get_status_of_all_updaters()
    return _json_response(json.dumps(status_info))


def _json_response(body, status=200):
    return Response(body, status=status, mimetype=APPLICATION_JSON_TYPE)


def _get_status_of_all_updaters():
    updaters = sync_manager.get_index_updaters()
    updater_status_info = {updater.id: _get_updater_status(updater) for updater in updaters}
    return {
        'polling_interval': app.config['POLLING_INTERVAL_SECS'],
        'status': updater_status_info,
    }


def _get_updater_status(updater):
    return {
        'last_successful_sync_time': _format_optional_date(updater.last_successful_sync_time),
        'last_unsuccessful_sync_time': _format_optional_date(updater.last_unsuccessful_sync_time),
        'last_title_modification_date': _format_optional_date(
            updater.last_title_modification_date
        ),
        'last_title_number': updater.last_updated_title_number,
        'is_busy': sync_manager.is_index_updater_busy(updater),
        'index_name': updater.index_name,
        'doc_type': updater.doc_type,
    }


def _format_optional_date(date):
    if date:
        return format_date_with_millis(date)
    else:
        return None


def _check_postgresql_connection():
    """Checks PostgreSQL connection and returns a list of errors"""
    try:
        # Request a page of data just to see if the database responds properly
        page_reader.get_next_data_page('', '2100-01-01', 1)
        return []
    except Exception as e:
        error_message = 'Problem talking to PostgreSQL: {0}'.format(str(e))
        return [error_message]


def _check_elasticsearch_connection():
    """Checks elasticsearch connection and returns a list of errors"""
    try:
        status = es_utils.get_cluster_info()['status']
        if status == 200:
            return []
        else:
            return ['Unexpected elasticsearch status: {}'.format(status)]
    except Exception as e:
        return ['Problem talking to elasticsearch: {0}'.format(str(e))]


if not CONFIG_DICT.get('TESTING', False):
    sync_manager.start()
