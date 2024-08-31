import json
import os

import fsspec

from daily_chart_generator.index import handler


def test_handle_event():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, 'test-data.csv')
    with open(file_path, 'rb') as file_in:
        with fsspec.open('s3://sti-bus-delays/actual-data/test-data.csv', 'wb') as file_out:
            file_out.write(file_in.read())

    actual_http_response = handler({
        'detail': {
            'bucket': {
                'name': 'sti-bus-delays'
            },
            'object': {
                'key': 'actual-data/test-data.csv'
            }
        }
    }, {})

    assert actual_http_response['statusCode'] == 200
    assert actual_http_response['headers']['Content-Type'] == 'application/json'
    body = json.loads(actual_http_response['body'])
    assert body['message'] == 'Daily chart generated'
    assert body['path'].startswith('s3://sti-bus-delays/daily-charts/')
    assert body['path'].endswith('.png')
