from io import StringIO

import pandas as pd
import pytest
import os

from actual_data_downloader.index import get_actual_data, process_data, save_csv, generate_path, generate_filename, \
    get_dataset_day, load_and_save_as_csv

DELAYED_STI_THUN_STATION_DF = pd.DataFrame(
    [{'BETRIEBSTAG': pd.Timestamp('2024-08-23 00:00:00'),
      'AN_PROGNOSE': pd.Timestamp('2024-08-23 06:47:00'),
      'ANKUNFTSZEIT': pd.Timestamp('2024-08-23 06:46:00'),
      'BETREIBER_ABK': 'STI',
      'AN_PROGNOSE_STATUS': 'REAL',
      'HALTESTELLEN_NAME': 'Thun, Bahnhof'
      }]
)

DELAYED_STI_THUN_STATION_CSV = """BETRIEBSTAG;FAHRT_BEZEICHNER;BETREIBER_ID;BETREIBER_ABK;BETREIBER_NAME;PRODUKT_ID;LINIEN_ID;LINIEN_TEXT;UMLAUF_ID;VERKEHRSMITTEL_TEXT;ZUSATZFAHRT_TF;FAELLT_AUS_TF;BPUIC;HALTESTELLEN_NAME;ANKUNFTSZEIT;AN_PROGNOSE;AN_PROGNOSE_STATUS;ABFAHRTSZEIT;AB_PROGNOSE;AB_PROGNOSE_STATUS;DURCHFAHRT_TF;AN_VERSPAETUNG_MIN
    23.08.2024;85:146:170903-02196-1;85:146;STI;STI Bus AG;Bus;85:146:1;1;15;B;false;false;8507180;Thun, Bahnhof;23.08.2024 06:46;23.08.2024 06:47:00;REAL;23.08.2024 06:49;23.08.2024 06:46:54;REAL;false"""


def test_get_actual_data():
    actual_df = get_actual_data(StringIO(DELAYED_STI_THUN_STATION_CSV))

    assert actual_df['BETRIEBSTAG'].iloc[0] == pd.Timestamp('2024-08-23 00:00:00')
    assert actual_df['FAHRT_BEZEICHNER'].iloc[0] == '85:146:170903-02196-1'
    assert actual_df['BETREIBER_ID'].iloc[0] == '85:146'
    assert actual_df['BETREIBER_ABK'].iloc[0] == 'STI'
    assert actual_df['BETREIBER_NAME'].iloc[0] == 'STI Bus AG'
    assert actual_df['PRODUKT_ID'].iloc[0] == 'Bus'
    assert actual_df['LINIEN_ID'].iloc[0] == '85:146:1'
    assert actual_df['LINIEN_TEXT'].iloc[0] == '1'
    assert actual_df['UMLAUF_ID'].iloc[0] == '15'
    assert actual_df['VERKEHRSMITTEL_TEXT'].iloc[0] == 'B'
    assert actual_df['ZUSATZFAHRT_TF'].iloc[0] == False
    assert actual_df['FAELLT_AUS_TF'].iloc[0] == False
    assert actual_df['BPUIC'].iloc[0] == 8507180
    assert actual_df['HALTESTELLEN_NAME'].iloc[0] == 'Thun, Bahnhof'
    assert actual_df['ANKUNFTSZEIT'].iloc[0] == pd.Timestamp('2024-08-23 06:46:00')
    assert actual_df['AN_PROGNOSE'].iloc[0] == pd.Timestamp('2024-08-23 06:47:00')
    assert actual_df['AN_PROGNOSE_STATUS'].iloc[0] == 'REAL'
    assert actual_df['ABFAHRTSZEIT'].iloc[0] == '23.08.2024 06:49'
    assert actual_df['AB_PROGNOSE'].iloc[0] == '23.08.2024 06:46:54'
    assert actual_df['AB_PROGNOSE_STATUS'].iloc[0] == 'REAL'
    assert actual_df['DURCHFAHRT_TF'].iloc[0] == False


def test_process_data():
    actual_df = process_data(DELAYED_STI_THUN_STATION_DF)

    assert len(actual_df.index) == 1
    assert actual_df['AN_VERSPAETUNG_MIN'].iloc[0] == 1


def test_operator_not_sti():
    operator_not_sti_df = DELAYED_STI_THUN_STATION_DF.copy()
    operator_not_sti_df['BETREIBER_ABK'] = 'NOT_STI'

    actual_df = process_data(operator_not_sti_df)

    assert len(actual_df.index) == 0


def test_arrival_forecast_not_real():
    arrival_forecast_not_real_df = DELAYED_STI_THUN_STATION_DF.copy()
    arrival_forecast_not_real_df['AN_PROGNOSE_STATUS'] = 'NOT_REAL'

    actual_df = process_data(arrival_forecast_not_real_df)

    assert len(actual_df.index) == 0


def test_arrival_not_before_forecast():
    arrival_not_before_forecast = DELAYED_STI_THUN_STATION_DF.copy()
    arrival_not_before_forecast['AN_PROGNOSE'] = pd.Timestamp('2024-08-23 06:46:00'),
    arrival_not_before_forecast['ANKUNFTSZEIT'] = pd.Timestamp('2024-08-23 06:47:00')

    actual_df = process_data(arrival_not_before_forecast)

    assert len(actual_df.index) == 0


def test_station_not_thun_station():
    station_not_thun_station = DELAYED_STI_THUN_STATION_DF.copy()
    station_not_thun_station['HALTESTELLEN_NAME'] = 'Not Thun, Bahnhof'

    actual_df = process_data(station_not_thun_station)

    assert len(actual_df.index) == 0


def test_get_dataset_day():
    actual_dataset_day = get_dataset_day(DELAYED_STI_THUN_STATION_DF)

    assert actual_dataset_day == pd.Timestamp('2024-08-23 00:00:00')


def test_generate_path():
    actual_path = generate_path('path', pd.Timestamp('2024-08-23 00:00:00'))

    assert actual_path == 'path/2024/08'


def test_generate_filename():
    actual_filename = generate_filename(pd.Timestamp('2024-08-23 00:00:00'), 'test.csv')

    assert actual_filename == '2024-08-23_test.csv'


@pytest.fixture(scope='session')
def csv_data_folder(tmp_path_factory):
    fn = tmp_path_factory.mktemp('csv_data')
    return fn


def test_save_csv(csv_data_folder):
    os.makedirs(f"{csv_data_folder}/2024/08", exist_ok=True)

    path = save_csv(DELAYED_STI_THUN_STATION_DF, f"{csv_data_folder}/2024/08", "2024-08-23_test.csv")

    assert path == f"{csv_data_folder}/2024/08/2024-08-23_test.csv"


def test_load_and_save_as_csv(csv_data_folder):
    os.makedirs(f"{csv_data_folder}/2024/08", exist_ok=True)

    path = load_and_save_as_csv(StringIO(DELAYED_STI_THUN_STATION_CSV), csv_data_folder, "test.csv")

    assert path == f"{csv_data_folder}/2024/08/2024-08-23_test.csv"
