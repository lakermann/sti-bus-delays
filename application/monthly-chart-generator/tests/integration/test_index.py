import json
import os

import fsspec
import pytest

from monthly_chart_generator.index import handler


@pytest.mark.filterwarnings(
    "ignore:in a future version of UPath this will be set to None unless the filesystem is local (or caches locally)*:PendingDeprecationWarning")
def test_handle_event():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, 'test-data.csv')
    with open(file_path, 'rb') as file_in:
        with fsspec.open('s3://sti-bus-delays/actual-data/2024/08/2024-08-25-test-data.csv', 'wb') as file_out:
            file_out.write(file_in.read())

    actual_http_response = handler({
        'detail': {
            'bucket': {
                'name': 'sti-bus-delays'
            },
            'object': {
                'key': 'actual-data/2024/08/2024-08-25-test-data.csv'
            }
        }
    }, {})

    assert actual_http_response['statusCode'] == 200
    assert actual_http_response['headers']['Content-Type'] == 'application/json'
    body = json.loads(actual_http_response['body'])
    assert body['message'] == 'Monthly charts generated'
    assert all(
        [path.startswith('s3://sti-bus-delays/monthly-charts/') and path.endswith('.png') for path in body['path']])
