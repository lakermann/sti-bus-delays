from io import StringIO

import pandas as pd
import pytest

from monthly_chart_generator.index import read_csv, read_all_csv_files, \
    read_all_files_in_input_path_and_generate_charts, generate_chart, generate_filepath

DELAYED_STI_THUN_STATION_CSV = """BETRIEBSTAG;FAHRT_BEZEICHNER;LINIEN_TEXT;ANKUNFTSZEIT;AN_VERSPAETUNG_MIN
    2024-08-23;85:146:170903-02196-1;1;2024-08-23 06:46;1
    2024-08-24;85:146:170903-02196-1;2;2024-08-23 06:46;1"""


@pytest.fixture(scope='function')
def temp_data_folder(tmp_path_factory):
    fn = tmp_path_factory.mktemp('test')
    return fn


def test_read_all_files_in_input_path_and_generate_charts(temp_data_folder):
    csv_file_path = f"{temp_data_folder}/test1.csv"
    pd.read_csv(StringIO(DELAYED_STI_THUN_STATION_CSV), sep=';').to_csv(csv_file_path, sep=';', index=False)
    output_path = temp_data_folder
    output_file_name = 'test'

    actual_file_path_list = read_all_files_in_input_path_and_generate_charts(temp_data_folder, output_path,
                                                                             output_file_name)
    assert actual_file_path_list == [f"{temp_data_folder}/route-1/2024/2024-08_route-1_test.png",
                                     f"{temp_data_folder}/route-2/2024/2024-08_route-2_test.png"]


def test_read_all_csv_files(temp_data_folder):
    file_path1 = f"{temp_data_folder}/test1.csv"
    file_path2 = f"{temp_data_folder}/test2.csv"
    pd.read_csv(StringIO(DELAYED_STI_THUN_STATION_CSV), sep=';').to_csv(file_path1, sep=';', index=False)
    pd.read_csv(StringIO(DELAYED_STI_THUN_STATION_CSV), sep=';').to_csv(file_path2, sep=';', index=False)

    actual_pd = read_all_csv_files(temp_data_folder)

    assert len(actual_pd.index) == 4


def test_read_csv():
    actual_df = read_csv(StringIO(DELAYED_STI_THUN_STATION_CSV))

    assert actual_df['BETRIEBSTAG'].iloc[0] == pd.Timestamp('2024-08-23 00:00:00')
    assert actual_df['LINIEN_TEXT'].iloc[0] == '1'
    assert actual_df['ANKUNFTSZEIT'].iloc[0] == pd.Timestamp('2024-08-23 06:46:00')
    assert actual_df['AN_VERSPAETUNG_MIN'].iloc[0] == 1
    assert actual_df['FAHRT_BEZEICHNER'].iloc[0] == '85:146:170903-02196-1'
    assert actual_df['ANKUNFTSZEIT_GERUNDED_STUNDE'].iloc[0] == '06'
    assert actual_df['WOCHENTAG'].iloc[0] == 'Fri'


def test_generate_chart(temp_data_folder):
    df = read_csv(StringIO(DELAYED_STI_THUN_STATION_CSV))
    csv_file_path = f"{temp_data_folder}/test1.csv"
    pd.read_csv(StringIO(DELAYED_STI_THUN_STATION_CSV), sep=';').to_csv(csv_file_path, sep=';', index=False)

    actual_file_path = generate_chart(df, '1', temp_data_folder, 'test')

    assert actual_file_path == f"{temp_data_folder}/route-1/2024/2024-08_route-1_test.png"


def test_generate_filepath():
    timestamp = pd.Timestamp('2024-08-23')
    actual_filepath = generate_filepath('path', timestamp, 'filename', '1')

    assert actual_filepath == 'path/route-1/2024/2024-08_route-1_filename.png'
