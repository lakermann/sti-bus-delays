import os
import pandas as pd
import json

DATA_BUCKET_NAME = os.environ['DATA_BUCKET_NAME']

def get_actual_data(url):
    df = pd.read_csv(url,
                     sep=";",
                     dtype = {'BETRIEBSTAG': str,
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
    df['BETRIEBSTAG'] = pd.to_datetime(df['BETRIEBSTAG'], dayfirst = True)
    df['ANKUNFTSZEIT'] = pd.to_datetime(df['ANKUNFTSZEIT'], dayfirst = True)
    df['AN_PROGNOSE'] = pd.to_datetime(df['AN_PROGNOSE'], dayfirst = True)
    return df

def process_data(df):
    df['ANKUNFTSZEIT'] = pd.to_datetime(df['ANKUNFTSZEIT'], dayfirst = True)
    df['AN_PROGNOSE'] = pd.to_datetime(df['AN_PROGNOSE'], dayfirst = True)
    df['AN_VERSPAETUNG_MIN'] = (df['AN_PROGNOSE']-df['ANKUNFTSZEIT']).dt.seconds.div(60)
    return df.query('BETREIBER_ABK == "STI" & AN_PROGNOSE_STATUS == "REAL" & ANKUNFTSZEIT<AN_PROGNOSE & HALTESTELLEN_NAME == "Thun, Bahnhof"').sort_values(by='ANKUNFTSZEIT')

def save_csv(df):
    year = df['BETRIEBSTAG'].dt.year.values[0]
    month = df['BETRIEBSTAG'].dt.month.values[0]
    date = df['BETRIEBSTAG'].dt.date.values[0]

    df.to_csv(f"s3://{DATA_BUCKET_NAME}/actual-data/{year}/{month}/{date}_sti_thun_bahnhof.csv",
              sep=";",
              index=False)

def handler(event, context):
    df = get_actual_data("https://opentransportdata.swiss/de/dataset/istdaten/permalink")
    print(df.head(10))
    processed_df = process_data(df)
    save_csv(processed_df)
    return {
        'statusCode': 200,
        'body': json.dumps('Actual data saved to csv.')
    }