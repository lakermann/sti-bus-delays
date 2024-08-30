import json
import os

import fsspec

from monthly_chart_generator.index import handler


def test_handle_event():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, "test-data.csv")
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
    assert body['path'] == ['s3://sti-bus-delays/monthly-charts/route 1/2024/8_sti_thun_bahnhof.png.png',
                            's3://sti-bus-delays/monthly-charts/route 5/2024/8_sti_thun_bahnhof.png.png',
                            's3://sti-bus-delays/monthly-charts/route 6/2024/8_sti_thun_bahnhof.png.png',
                            's3://sti-bus-delays/monthly-charts/route 3/2024/8_sti_thun_bahnhof.png.png',
                            's3://sti-bus-delays/monthly-charts/route 43/2024/8_sti_thun_bahnhof.png.png',
                            's3://sti-bus-delays/monthly-charts/route 2/2024/8_sti_thun_bahnhof.png.png',
                            's3://sti-bus-delays/monthly-charts/route 32/2024/8_sti_thun_bahnhof.png.png',
                            's3://sti-bus-delays/monthly-charts/route 4/2024/8_sti_thun_bahnhof.png.png',
                            's3://sti-bus-delays/monthly-charts/route 21/2024/8_sti_thun_bahnhof.png.png',
                            's3://sti-bus-delays/monthly-charts/route 41/2024/8_sti_thun_bahnhof.png.png',
                            's3://sti-bus-delays/monthly-charts/route 25/2024/8_sti_thun_bahnhof.png.png',
                            's3://sti-bus-delays/monthly-charts/route 50/2024/8_sti_thun_bahnhof.png.png',
                            's3://sti-bus-delays/monthly-charts/route 55/2024/8_sti_thun_bahnhof.png.png',
                            's3://sti-bus-delays/monthly-charts/route 42/2024/8_sti_thun_bahnhof.png.png',
                            's3://sti-bus-delays/monthly-charts/route 51/2024/8_sti_thun_bahnhof.png.png',
                            's3://sti-bus-delays/monthly-charts/route 33/2024/8_sti_thun_bahnhof.png.png']
