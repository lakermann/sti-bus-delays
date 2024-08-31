import json

from actual_data_downloader.index import handler


def test_handle_event_without_dataset_url():
    actual_http_response = handler({}, {})

    assert actual_http_response['statusCode'] == 200
    assert actual_http_response['headers']['Content-Type'] == 'application/json'
    body = json.loads(actual_http_response['body'])
    assert body['message'] == 'Actual data saved'
    assert body['path'].startswith('s3://sti-bus-delays/actual-data/')
    assert body['path'].endswith('.csv')


def test_handle_event_with_custom_dataset_url():
    event = {
        'dataset-url': 'https://opentransportdata.swiss/de/dataset/istdaten/permalink'
    }

    actual_http_response = handler(event, {})

    assert actual_http_response['statusCode'] == 200
    assert actual_http_response['headers']['Content-Type'] == 'application/json'
    body = json.loads(actual_http_response['body'])
    assert body['message'] == 'Actual data saved'
    assert body['path'].startswith('s3://sti-bus-delays/actual-data/')
    assert body['path'].endswith('.csv')
