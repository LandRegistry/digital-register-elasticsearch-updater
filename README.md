# digital-register-elasticsearch-updater

## Run the server

### Run in dev mode

To run the server in dev mode, execute the following command:

    ./run_flask_dev.sh

### Run using gunicorn

To run the server using gunicorn, activate your virtual environment, add the application directory to python path
(e.g. `export PYTHONPATH=/vagrant/apps/digital-register-elasticsearch-updater/:$PYTHONPATH`) and execute the following commands:

    pip install gunicorn
    gunicorn -p /tmp/gunicorn-digital-register-elasticsearch-updater.pid service.server:app -c gunicorn_settings.py


## Using the API

### Using the healthcheck endpoint

Healthcheck endpoint can be used to check if the application is running. In order to call it,
execute the following command:

    curl http://localhost:8006

The expected response is an HTTP response with status 200 and `OK` in the body.

### Using the status endpoint

Status endpoint returns information about current index synchronisation state. Here's how to call it:

    curl http://localhost:8006/status

The response should look similar to the following:

    {
        "polling_interval": 2,
        "status": {
            "property-by-address-v1-updater": {
                "doc_type": "property_by_address",
                "index_name": "landregistry",
                "is_busy": false,
                "last_successful_sync_time": "2015-05-13T13:17:50.043+00",
                "last_unsuccessful_sync_time": null,
                "last_title_modification_date": "2015-05-13T13:10:33.392+00",
                "last_title_number": "AGL1234"
            },
            "property-by-postcode-v1-updater": {
                "doc_type": "property_by_postcode",
                "index_name": "landregistry",
                "is_busy": false,
                "last_successful_sync_time": "2015-05-13T13:17:50.035+00",
                "last_unsuccessful_sync_time": null,
                "last_title_modification_date": "2015-05-13T13:10:33.392+00",
                "last_title_number": "AGL1234"
            },
            "property-by-postcode-v2-updater": {
                "doc_type": "property_by_postcode_2",
                "index_name": "landregistry",
                "is_busy": false,
                "last_successful_sync_time": "2015-05-13T13:17:50.055+00",
                "last_unsuccessful_sync_time": null,
                "last_title_modification_date": "2015-05-13T13:10:33.392+00",
                "last_title_number": "AGL1234"
            }
        }
    }

`polling_interval` indicates how often, in seconds, the application synchronises elasticsearch with its source
data store.

`status` element contains key-value pairs where the keys are index updater IDs and the values
are objects with the following fields:

    index_name - name of the index populated by the updater
    doc_type - document type the updater is responsible for
    is_busy - indicates whether the updater is currently running
    last_successful_sync_time - starting time of the last successful synchronisation
    last_unsuccessful_sync_time - starting time of the last unsuccessful synchronisation attempt
    last_title_modification_date - 'last_modified' date of the recently processed title
    last_title_number - number of the recently processed title
