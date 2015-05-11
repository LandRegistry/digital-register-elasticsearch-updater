#!/usr/bin/env python
import atexit
import logging

from service import server

LOGGER = logging.getLogger(__name__)


@atexit.register
def handle_shutdown(*args, **kwargs):
    LOGGER.info("Stopped the server")

LOGGER.info("Starting the server")
server.run_app()
