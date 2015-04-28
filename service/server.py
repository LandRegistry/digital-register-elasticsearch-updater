import os
from service import sync_manager, app

# TODO: add status endpoint that lists indexes and their update statuses


def run_app():
    port = int(os.environ.get('PORT', 8006))
    app.run(host='0.0.0.0', port=port)
    pass

sync_manager.start()
