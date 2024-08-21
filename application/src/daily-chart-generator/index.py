import os
import pandas as pd
import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import json
import io
import s3fs

DATA_BUCKET_NAME = os.environ['DATA_BUCKET_NAME']

def read_csv(csv_file_path):
    df = pd.read_csv(csv_file_path,
                     sep=";",
                     dtype = {'BETRIEBSTAG': str,
                              'LINIEN_TEXT': str,
                              'ANKUNFTSZEIT': str,
                              'AN_VERSPAETUNG_MIN': float,
                              }
                     )

    df['BETRIEBSTAG'] = pd.to_datetime(df['BETRIEBSTAG'])
    df['ANKUNFTSZEIT'] = pd.to_datetime(df['ANKUNFTSZEIT'])
    return df

def generate_chart(df, path):
    pivot_table_df=df.pivot_table(index=['ANKUNFTSZEIT'],
                                  columns=['LINIEN_TEXT'],
                                  values=['AN_VERSPAETUNG_MIN'],
                                  aggfunc="max")

    year = df['BETRIEBSTAG'].dt.year.values[0]
    month = df['BETRIEBSTAG'].dt.month.values[0]
    date = df['BETRIEBSTAG'].dt.date.values[0]

    ax=pivot_table_df.plot(linestyle='none',
                           marker='o',
                           figsize=(15, 8),
                           title=f"Verspätungen der STI Busse am Bahnhof Thun - {date.strftime('%d.%m.%Y')} ({date.strftime('%a')})"
                           )

    ax.set_xlabel("ANKUNFTSZEIT GEMÄSS FAHRPLAN")
    ax.set_ylabel("VERSPÄTUNG (MIN)")
    ax.legend(["Linie " + str(s2) for (s1,s2) in pivot_table_df.columns.tolist()])
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.tick_params(axis='x', labelrotation=45)
    ax.set_ybound(lower=0, upper=90)

    fig = ax.get_figure()
    fig.text(0.9, 0.04, 'Datenquelle: https://opentransportdata.swiss/de/dataset/istdaten/',
             horizontalalignment='right',
             verticalalignment='bottom')

    img_data = io.BytesIO()
    fig.savefig(img_data, format='png')
    img_data.seek(0)

    s3 = s3fs.S3FileSystem(anon=False)  # Uses default credentials
    with s3.open(f"{path}{year}/{month}/{date}_sti_thun_bahnhof.png", 'wb') as f:
        f.write(img_data.getbuffer())

def handler(event, context):
    print(event)

    bucket_name = event["detail"]["bucket"]["name"]
    object_key = event["detail"]["object"]["key"]

    df = read_csv(f"s3://{bucket_name}/{object_key}")
    generate_chart(df, f"s3://{bucket_name}/daily-charts/")

    # for record in event['Records']:
    #     print(record["s3"]["bucket"]["name"])
    #     print(record["s3"]["object"]["key"])
    #
    #     bucket_name = record["s3"]["bucket"]["name"]
    #     object_key = record["s3"]["object"]["key"]
    #
    #     df = read_csv(f"s3://{bucket_name}/{object_key}")
    #     generate_chart(df, f"s3://{bucket_name}/daily-charts/")

    return {
        'statusCode': 200,
        'body': json.dumps('Generated daily chart.')
    }

