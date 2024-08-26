import io
import json
import os

import fsspec
import matplotlib.dates as mdates
import pandas as pd


def read_csv(csv_file_path):
    df = pd.read_csv(csv_file_path,
                     sep=";",
                     dtype={'BETRIEBSTAG': str,
                            'LINIEN_TEXT': str,
                            'ANKUNFTSZEIT': str,
                            'AN_VERSPAETUNG_MIN': float,
                            }
                     )

    df['BETRIEBSTAG'] = pd.to_datetime(df['BETRIEBSTAG'])
    df['ANKUNFTSZEIT'] = pd.to_datetime(df['ANKUNFTSZEIT'])
    return df


def pivot_data(df):
    return df.pivot_table(index=['ANKUNFTSZEIT'],
                          columns=['LINIEN_TEXT'],
                          values=['AN_VERSPAETUNG_MIN'],
                          aggfunc="max")


def get_dataset_day(df):
    return df['BETRIEBSTAG'].iloc[0]


def generate_chart(dataset_day, pivot_table_df):
    ax = pivot_table_df.plot(linestyle='none',
                             marker='o',
                             figsize=(15, 8),
                             title=f"STI Bus delays at 'Bahnhof Thun' station - {dataset_day.strftime('%d.%m.%Y')} ({dataset_day.strftime('%a')})",
                             )

    ax.set_xlabel("Arrival Time according to Timetable")
    ax.set_ylabel("Delay in Minutes")
    ax.legend(["Route " + str(s2) for (s1, s2) in pivot_table_df.columns.tolist()])
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.tick_params(axis='x', labelrotation=45)
    ax.set_ybound(lower=0, upper=90)

    return ax


def generate_path(path, timestamp):
    return f"{path}/{timestamp.strftime('%Y/%m')}"


def generate_filename(timestamp, file_name):
    date = timestamp.strftime('%Y-%m-%d')
    return f"{date}_{file_name}"


def save_figure(ax, dataset_day, output_path, file_name):
    fig = ax.get_figure()
    fig.text(0.9, 0.04, 'Data source: https://opentransportdata.swiss/de/dataset/istdaten/',
             horizontalalignment='right',
             verticalalignment='bottom')
    img_data = io.BytesIO()
    fig.savefig(img_data, format='png')
    img_data.seek(0)

    path = generate_path(output_path, dataset_day)
    file_name = generate_filename(dataset_day, file_name)
    file_path = f"{path}/{file_name}.png"

    of = fsspec.open(file_path, 'wb')
    with of as f:
        f.write(img_data.getbuffer())

    return file_path


def read_csv_and_generate_chart(input_path, output_path, filename):
    df = read_csv(input_path)
    dataset_day = get_dataset_day(df)
    pivot_table_df = pivot_data(df)
    fig = generate_chart(dataset_day, pivot_table_df)
    file_path = save_figure(fig, dataset_day, output_path, filename)
    return file_path


def handler(event, context):
    bucket_name = event["detail"]["bucket"]["name"]
    object_key = event["detail"]["object"]["key"]
    output_path = os.getenv('OUTPUT_PATH')
    file_name = os.getenv('OUTPUT_FILE_NAME')
    path = read_csv_and_generate_chart(f"s3://{bucket_name}/{object_key}", f"s3://{output_path}", file_name)

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps({
            'message': 'Daily chart generated',
            'path': path
        })
    }
