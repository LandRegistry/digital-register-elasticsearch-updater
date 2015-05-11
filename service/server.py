import json
import logging
import os
from flask import Response
from config import CONFIG_DICT

from service import sync_manager, app
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


@app.route('/', methods=['GET'])
def healthcheck():
    return "OK"


@app.route('/status', methods=['GET'])
def status():
    status_info = _get_status_of_all_updaters()
    return _json_response(json.dumps(status_info))


def run_app():
    port = int(os.environ.get('PORT', 8006))
    app.run(host='0.0.0.0', port=port)


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
        'is_busy': sync_manager.is_index_updater_busy(updater),
        'index_name': updater.index_name,
        'doc_type': updater.doc_type,
    }


def _format_optional_date(date):
    if date:
        return format_date_with_millis(date)
    else:
        return None


if not CONFIG_DICT.get('TESTING', False):
    sync_manager.start()
