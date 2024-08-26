from actual_data_downloader.index import handler
import json


def test_handle_event_without_dataset_url():
    http_response = handler({}, {})

    assert http_response['statusCode'] == 200
    assert http_response['headers']['Content-Type'] == 'application/json'
    assert json.loads(http_response['body'])['message'] == 'Actual data saved'


def test_handle_event_with_custom_dataset_url():
    event = {
        'dataset-url': 'https://opentransportdata.swiss/de/dataset/istdaten/permalink'
    }

    http_response = handler(event, {})

    assert http_response['statusCode'] == 200
    assert http_response['headers']['Content-Type'] == 'application/json'
    assert json.loads(http_response['body'])['message'] == 'Actual data saved'
