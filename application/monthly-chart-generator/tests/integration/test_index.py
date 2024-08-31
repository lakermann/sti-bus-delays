import json
import os
import pytest

import fsspec

from monthly_chart_generator.index import handler


@pytest.mark.filterwarnings(
    "ignore:in a future version of UPath this will be set to None unless the filesystem is local (or caches locally)*:PendingDeprecationWarning")
def test_handle_event():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, 'test-data.csv')
    with open(file_path, 'rb') as file_in:
        with fsspec.open('s3://sti-bus-delays/actual-data/2024/08/2024-08-25-test-data.csv', 'wb') as file_out:
            file_out.write(file_in.read())

    http_response = handler({
        'detail': {
            'bucket': {
                'name': 'sti-bus-delays'
            },
            'object': {
                'key': 'actual-data/2024/08/2024-08-25-test-data.csv'
            }
        }
    }, {})

    assert http_response['statusCode'] == 200
    body = json.loads(http_response['body'])
    assert body['message'] == 'Monthly charts generated'
    assert body['path'] == ['s3://sti-bus-delays/monthly-charts/route-1/2024/2024-08_route-1_sti-thun-bahnhof.png',
                            's3://sti-bus-delays/monthly-charts/route-5/2024/2024-08_route-5_sti-thun-bahnhof.png',
                            's3://sti-bus-delays/monthly-charts/route-6/2024/2024-08_route-6_sti-thun-bahnhof.png',
                            's3://sti-bus-delays/monthly-charts/route-3/2024/2024-08_route-3_sti-thun-bahnhof.png',
                            's3://sti-bus-delays/monthly-charts/route-43/2024/2024-08_route-43_sti-thun-bahnhof.png',
                            's3://sti-bus-delays/monthly-charts/route-2/2024/2024-08_route-2_sti-thun-bahnhof.png',
                            's3://sti-bus-delays/monthly-charts/route-32/2024/2024-08_route-32_sti-thun-bahnhof.png',
                            's3://sti-bus-delays/monthly-charts/route-4/2024/2024-08_route-4_sti-thun-bahnhof.png',
                            's3://sti-bus-delays/monthly-charts/route-21/2024/2024-08_route-21_sti-thun-bahnhof.png',
                            's3://sti-bus-delays/monthly-charts/route-41/2024/2024-08_route-41_sti-thun-bahnhof.png',
                            's3://sti-bus-delays/monthly-charts/route-25/2024/2024-08_route-25_sti-thun-bahnhof.png',
                            's3://sti-bus-delays/monthly-charts/route-50/2024/2024-08_route-50_sti-thun-bahnhof.png',
                            's3://sti-bus-delays/monthly-charts/route-55/2024/2024-08_route-55_sti-thun-bahnhof.png',
                            's3://sti-bus-delays/monthly-charts/route-42/2024/2024-08_route-42_sti-thun-bahnhof.png',
                            's3://sti-bus-delays/monthly-charts/route-51/2024/2024-08_route-51_sti-thun-bahnhof.png',
                            's3://sti-bus-delays/monthly-charts/route-33/2024/2024-08_route-33_sti-thun-bahnhof.png']
