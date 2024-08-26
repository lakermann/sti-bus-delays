from daily_chart_generator.index import handler
import json
import fsspec
import os


def test_handle_event():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, "test-data.csv")
    with open(file_path, 'rb') as file_in:
        with fsspec.open('s3://sti-bus-delays/actual-data/test-data.csv', 'wb') as file_out:
            file_out.write(file_in.read())

    http_response = handler({
        'detail': {
            'bucket': {
                'name': 'sti-bus-delays'
            },
            'object': {
                'key': 'actual-data/test-data.csv'
            }
        }
    }, {})

    assert http_response['statusCode'] == 200
    assert http_response['headers']['Content-Type'] == 'application/json'
    assert json.loads(http_response['body'])['message'] == 'Daily chart generated'
