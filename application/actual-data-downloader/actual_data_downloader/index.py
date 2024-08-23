import json
import os
import pandas as pd


def get_actual_data(url):
    df = pd.read_csv(url,
                     sep=';',
                     dtype={'BETRIEBSTAG': str,
                            'FAHRT_BEZEICHNER': str,
                            'BETREIBER_ID': str,
                            'BETREIBER_ABK': str,
                            'BETREIBER_NAME': str,
                            'PRODUKT_ID': str,
                            'LINIEN_ID': str,
                            'LINIEN_TEXT': str,
                            'UMLAUF_ID': str,
                            'VERKEHRSMITTEL_TEXT': str,
                            'ZUSATZFAHRT_TF': bool,
                            'FAELLT_AUS_TF': bool,
                            'BPUIC': int,
                            'HALTESTELLEN_NAME': str,
                            'ANKUNFTSZEIT': str,
                            'AN_PROGNOSE': str,
                            'AN_PROGNOSE_STATUS': str,
                            'ABFAHRTSZEIT': str,
                            'AB_PROGNOSE': str,
                            'AB_PROGNOSE_STATU': str,
                            'DURCHFAHRT_TF': bool},
                     )
    df['BETRIEBSTAG'] = pd.to_datetime(df['BETRIEBSTAG'], dayfirst=True)
    df['ANKUNFTSZEIT'] = pd.to_datetime(df['ANKUNFTSZEIT'], dayfirst=True)
    df['AN_PROGNOSE'] = pd.to_datetime(df['AN_PROGNOSE'], dayfirst=True)
    return df


def process_data(df):
    df['AN_VERSPAETUNG_MIN'] = (df['AN_PROGNOSE'] - df['ANKUNFTSZEIT']).dt.seconds.div(60)
    return (df.query(
        'BETREIBER_ABK == "STI" & AN_PROGNOSE_STATUS == "REAL" & ANKUNFTSZEIT < AN_PROGNOSE & HALTESTELLEN_NAME == "Thun, Bahnhof"')
            .sort_values(by='ANKUNFTSZEIT'))


def save_csv(df, output_path, file_name):
    subpath = df['BETRIEBSTAG'].iloc[0].strftime('%Y/%m')
    date = df['BETRIEBSTAG'].iloc[0].strftime('%Y-%m-%d')
    path = f"{output_path}/{subpath}/{date}_{file_name}"
    df.to_csv(path,
              sep=';',
              index=False)
    return path


def handler(event, context):
    default_dataset_url = os.getenv('DEFAULT_DATASET_URL')
    output_path = os.getenv('OUTPUT_PATH')
    file_name = os.getenv('OUTPUT_FILE_NAME')
    dataset_url = event.get('dataset-url', default_dataset_url)

    actual_df = get_actual_data(dataset_url)
    processed_df = process_data(actual_df)
    path = save_csv(processed_df, output_path, file_name)

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps({
            'message': f"Actual data saved in {path}."
        })
    }
