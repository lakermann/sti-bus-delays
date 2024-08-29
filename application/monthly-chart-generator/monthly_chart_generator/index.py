import io
import json
import os
from calendar import day_abbr

import matplotlib.pyplot as plt
import pandas as pd
import s3fs


def read_csv(filepath_or_buffer):
    df = pd.read_csv(filepath_or_buffer,
                     sep=";",
                     dtype={'BETRIEBSTAG': str,
                            'LINIEN_TEXT': str,
                            'ANKUNFTSZEIT': str,
                            'AN_VERSPAETUNG_MIN': float,
                            'FAHRT_BEZEICHNER': str
                            }
                     )

    df['BETRIEBSTAG'] = pd.to_datetime(df['BETRIEBSTAG'])
    df['ANKUNFTSZEIT'] = pd.to_datetime(df['ANKUNFTSZEIT'])
    df['ANKUNFTSZEIT_GERUNDED_STUNDE'] = df['ANKUNFTSZEIT'].dt.floor(freq='60min').dt.strftime('%H')
    df['WOCHENTAG'] = pd.Categorical(pd.to_datetime(df['ANKUNFTSZEIT']).dt.strftime('%a'),
                                     categories=list(day_abbr), ordered=True)
    return df


def read_csv_files(path, csv_file_path_list):
    data = []
    for file in csv_file_path_list:
        data.append(read_csv(f"{path}{file}"))

    return pd.concat(data)


def generate_chart(df, line, path, file_name):
    df1 = df.query(f"LINIEN_TEXT == '{line}'")
    fig, axes = plt.subplots(3, 3, sharex=False, sharey=True)
    fig.delaxes(axes.flatten()[7])
    fig.delaxes(axes.flatten()[8])
    fig.set_size_inches(15, 10)

    start_date = df['BETRIEBSTAG'].min()
    end_date = df['BETRIEBSTAG'].max()

    for idx, day in enumerate(df.sort_values(by=['WOCHENTAG'], ascending=True)['WOCHENTAG'].unique()):
        subplot_ax = (axes.flatten())[idx]
        daily_df = df1.query(f"WOCHENTAG == '{day}'")

        daily_df.boxplot(
            column=['AN_VERSPAETUNG_MIN'],
            by=['ANKUNFTSZEIT_GERUNDED_STUNDE'],
            ax=subplot_ax,
            return_type='both',
            patch_artist=True,
        )

        labels = daily_df.sort_values(by=['ANKUNFTSZEIT_GERUNDED_STUNDE'], ascending=True)['ANKUNFTSZEIT_GERUNDED_STUNDE'].unique()
        subplot_ax.set_xticklabels(labels)
        subplot_ax.set_title(f"{day}")
        subplot_ax.set_xlabel('Arrival Time according to Timetable')
        subplot_ax.set_ylabel("Delay in Minutes")

    fig.text(0.9, 0.04, 'Data source: https://opentransportdata.swiss/de/dataset/istdaten/',
             horizontalalignment='right',
             verticalalignment='bottom',
             fontsize=8)

    plt.subplots_adjust(wspace=0.25, hspace=0.5)
    plt.suptitle(
        f"STI Bus delays at 'Bahnhof Thun' station \n Route {line} ({start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')})",
        fontsize=15)

    img_data = io.BytesIO()
    fig.savefig(img_data, format='png')
    plt.close(fig)
    img_data.seek(0)

    year = df['BETRIEBSTAG'].dt.year.values[0]
    month = df['BETRIEBSTAG'].dt.month.values[0]

    s3 = s3fs.S3FileSystem(anon=False)  # Uses default credentials
    with s3.open(f"{path}/Linie {line}/{year}/{month}_{file_name}", 'wb') as f:
        f.write(img_data.getbuffer())


def generate_chart_for_all_routes(df, path, file_name):
    for route in df['LINIEN_TEXT'].unique():
        generate_chart(df, route, path, file_name)


def read_all_files_for_month(bucket_name, year, month):
    s3 = s3fs.S3FileSystem(anon=False)
    files = s3.glob(f"s3://{bucket_name}/actual-data/{year}/{month}/*.csv")
    return read_csv_files(f"s3://", files)


def read_all_files_for_month_and_generate_charts(bucket_name, year, month, output_path, file_name):
    read_all_files_for_month(bucket_name, year, month)
    df = read_all_files_for_month(bucket_name, year, month, )
    generate_chart_for_all_routes(df, f"s3://{output_path}", file_name)


def handler(event, context):
    output_path = os.getenv('OUTPUT_PATH')
    file_name = os.getenv('OUTPUT_FILE_NAME')

    bucket_name = event["detail"]["bucket"]["name"]
    year = event["detail"]["object"]["key"].split("/")[1]
    month = event["detail"]["object"]["key"].split("/")[2]

    read_all_files_for_month_and_generate_charts(bucket_name, year, month, output_path, file_name)

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Monthly charts generated',
            'path': 'tbd'
        })
    }
