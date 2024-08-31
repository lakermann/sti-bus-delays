import os
from io import StringIO

import numpy as np
import pandas as pd
import pytest

from daily_chart_generator.index import read_csv, pivot_data, generate_chart, \
    generate_filepath, read_csv_and_generate_chart, get_dataset_day

DELAYED_STI_THUN_STATION_CSV = """BETRIEBSTAG;FAHRT_BEZEICHNER;BETREIBER_ID;BETREIBER_ABK;BETREIBER_NAME;PRODUKT_ID;LINIEN_ID;LINIEN_TEXT;UMLAUF_ID;VERKEHRSMITTEL_TEXT;ZUSATZFAHRT_TF;FAELLT_AUS_TF;BPUIC;HALTESTELLEN_NAME;ANKUNFTSZEIT;AN_PROGNOSE;AN_PROGNOSE_STATUS;ABFAHRTSZEIT;AB_PROGNOSE;AB_PROGNOSE_STATUS;DURCHFAHRT_TF;AN_VERSPAETUNG_MIN;AN_VERSPAETUNG_MIN
     2024-08-23;85:146:170903-02196-1;85:146;STI;STI Bus AG;Bus;85:146:1;1;15;B;false;false;8507180;Thun, Bahnhof;2024-08-23 06:46;2024-08-23 06:47:00;REAL;2024-08-23 06:49;2024-08-23 06:46:54;REAL;false;1"""

DELAYED_STI_THUN_STATION_PIVOT = pd.DataFrame(
    [{'ANKUNFTSZEIT': pd.Timestamp('2024-08-23 06:46:00'),
      'LINIEN_TEXT': '1',
      'AN_VERSPAETUNG_MIN': 1,
      },
     {'ANKUNFTSZEIT': pd.Timestamp('2024-08-23 06:46:00'),
      'LINIEN_TEXT': '1',
      'AN_VERSPAETUNG_MIN': 2,
      },
     {'ANKUNFTSZEIT': pd.Timestamp('2024-08-23 06:47:00'),
      'LINIEN_TEXT': '2',
      'AN_VERSPAETUNG_MIN': 3,
      }]
)


@pytest.fixture(scope='function')
def temp_data_folder(tmp_path_factory):
    fn = tmp_path_factory.mktemp('test')
    return fn


def test_read_csv_and_generate_chart(temp_data_folder):
    os.makedirs(f"{temp_data_folder}/daily-charts/2024/08", exist_ok=True)

    actual_path = read_csv_and_generate_chart(StringIO(DELAYED_STI_THUN_STATION_CSV),
                                              f"{temp_data_folder}/daily-charts",
                                              "test")

    assert actual_path == f"{temp_data_folder}/daily-charts/2024/08/2024-08-23_test.png"


def test_read_csv():
    actual_df = read_csv(StringIO(DELAYED_STI_THUN_STATION_CSV))

    assert actual_df['BETRIEBSTAG'].iloc[0] == pd.Timestamp('2024-08-23 00:00:00')
    assert actual_df['BETRIEBSTAG'].iloc[0] == pd.Timestamp('2024-08-23 00:00:00')
    assert actual_df['FAHRT_BEZEICHNER'].iloc[0] == '85:146:170903-02196-1'
    assert actual_df['BETREIBER_ID'].iloc[0] == '85:146'
    assert actual_df['BETREIBER_ABK'].iloc[0] == 'STI'
    assert actual_df['BETREIBER_NAME'].iloc[0] == 'STI Bus AG'
    assert actual_df['PRODUKT_ID'].iloc[0] == 'Bus'
    assert actual_df['LINIEN_ID'].iloc[0] == '85:146:1'
    assert actual_df['LINIEN_TEXT'].iloc[0] == '1'
    assert actual_df['UMLAUF_ID'].iloc[0] == 15
    assert actual_df['VERKEHRSMITTEL_TEXT'].iloc[0] == 'B'
    assert actual_df['ZUSATZFAHRT_TF'].iloc[0] == False
    assert actual_df['FAELLT_AUS_TF'].iloc[0] == False
    assert actual_df['BPUIC'].iloc[0] == 8507180
    assert actual_df['HALTESTELLEN_NAME'].iloc[0] == 'Thun, Bahnhof'
    assert actual_df['ANKUNFTSZEIT'].iloc[0] == pd.Timestamp('2024-08-23 06:46:00')
    assert actual_df['AN_PROGNOSE'].iloc[0] == '2024-08-23 06:47:00'
    assert actual_df['AN_PROGNOSE_STATUS'].iloc[0] == 'REAL'
    assert actual_df['ABFAHRTSZEIT'].iloc[0] == '2024-08-23 06:49'
    assert actual_df['AB_PROGNOSE'].iloc[0] == '2024-08-23 06:46:54'
    assert actual_df['AB_PROGNOSE_STATUS'].iloc[0] == 'REAL'
    assert actual_df['DURCHFAHRT_TF'].iloc[0] == False
    assert actual_df['AN_VERSPAETUNG_MIN'].iloc[0] == 1


def test_get_dataset_day():
    df = pd.DataFrame([{'BETRIEBSTAG': pd.Timestamp('2024-08-23 00:00:00')}])

    actual_dataset_day = get_dataset_day(df)

    assert actual_dataset_day == pd.Timestamp('2024-08-23 00:00:00')


def test_pivot_data():
    actual_df = pivot_data(DELAYED_STI_THUN_STATION_PIVOT)

    assert actual_df[('AN_VERSPAETUNG_MIN', '1')]['2024-08-23 06:46:00'] == 2
    assert np.isnan(actual_df[('AN_VERSPAETUNG_MIN', '1')]['2024-08-23 06:47:00'])
    assert np.isnan(actual_df[('AN_VERSPAETUNG_MIN', '2')]['2024-08-23 06:46:00'])
    assert actual_df[('AN_VERSPAETUNG_MIN', '2')]['2024-08-23 06:47:00'] == 3


def test_generate_chart(temp_data_folder):
    dataset_pivot = pivot_data(DELAYED_STI_THUN_STATION_PIVOT)
    dataset_day = pd.Timestamp('2024-08-23 06:46:00')

    actual_file_path = generate_chart(dataset_pivot, dataset_day, temp_data_folder, "test")

    assert actual_file_path == f"{temp_data_folder}/2024/08/2024-08-23_test.png"


def test_generate_generate_filepath():
    actual_filename = generate_filepath("path", pd.Timestamp('2024-08-23 00:00:00'), 'filename')

    assert actual_filename == 'path/2024/08/2024-08-23_filename.png'
