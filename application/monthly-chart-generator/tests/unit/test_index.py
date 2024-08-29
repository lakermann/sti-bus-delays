from io import StringIO

import pandas as pd

from monthly_chart_generator.index import read_csv

DELAYED_STI_THUN_STATION_CSV = """BETRIEBSTAG;FAHRT_BEZEICHNER;LINIEN_TEXT;ANKUNFTSZEIT;AN_VERSPAETUNG_MIN
2024-08-23;85:146:170903-02196-1;1;2024-08-23 06:46;1"""


def test_read_csv():
    actual_df = read_csv(StringIO(DELAYED_STI_THUN_STATION_CSV))
    assert actual_df['BETRIEBSTAG'].iloc[0] == pd.Timestamp('2024-08-23 00:00:00')
    assert actual_df['LINIEN_TEXT'].iloc[0] == '1'
    assert actual_df['ANKUNFTSZEIT'].iloc[0] == pd.Timestamp('2024-08-23 06:46:00')
    assert actual_df['AN_VERSPAETUNG_MIN'].iloc[0] == 1
    assert actual_df['FAHRT_BEZEICHNER'].iloc[0] == '85:146:170903-02196-1'
    assert actual_df['ANKUNFTSZEIT_GERUNDED_STUNDE'].iloc[0] == '06'
    assert actual_df['WOCHENTAG'].iloc[0] == 'Fri'
