import io
import json
import os
from calendar import day_abbr

import fsspec
import matplotlib.pyplot as plt
import pandas as pd
from upath import UPath


def handler(event, context):
    output_path = os.getenv('OUTPUT_PATH')
    output_file_name = os.getenv('OUTPUT_FILE_NAME')

    bucket_name = event['detail']['bucket']['name']
    path = os.path.dirname(event['detail']['object']['key'])

    output_file_path_list = read_all_files_in_input_path_and_generate_charts(f"s3://{bucket_name}/{path}", output_path,
                                                                             output_file_name)

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Monthly charts generated',
            'path': output_file_path_list
        })
    }


def read_all_files_in_input_path_and_generate_charts(input_path, output_path, output_file_name):
    df = read_all_csv_files(input_path)
    return [generate_chart(df, route, output_path, output_file_name) for route in df['LINIEN_TEXT'].unique()]


def read_all_csv_files(input_path):
    upath = UPath(input_path)
    files = upath.glob('*.csv')
    return pd.concat(
        [read_csv(file) for file in files]
    )


def read_csv(filepath_or_buffer):
    df = pd.read_csv(filepath_or_buffer,
                     sep=';',
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


def generate_chart(df, route, output_path, output_file_name):
    start_date = df['BETRIEBSTAG'].min()
    end_date = df['BETRIEBSTAG'].max()

    delays_per_route_df = df.query(f"LINIEN_TEXT == '{route}'")
    fig, axes = plt.subplots(3, 3, sharex=False, sharey=True)
    flatten_axes = axes.flatten()
    fig.delaxes(flatten_axes[7])
    fig.delaxes(flatten_axes[8])
    fig.set_size_inches(15, 10)

    for idx, day in enumerate(delays_per_route_df.sort_values(by=['WOCHENTAG'], ascending=True)['WOCHENTAG'].unique()):
        subplot_ax = flatten_axes[idx]
        delays_per_route_per_day_df = delays_per_route_df.query(f"WOCHENTAG == '{day}'")

        delays_per_route_per_day_df.boxplot(
            column=['AN_VERSPAETUNG_MIN'],
            by=['ANKUNFTSZEIT_GERUNDED_STUNDE'],
            ax=subplot_ax,
            return_type='both',
            patch_artist=True,
        )

        subplot_ax.set_title(day)
        labels = delays_per_route_per_day_df.sort_values(by=['ANKUNFTSZEIT_GERUNDED_STUNDE'], ascending=True)[
            'ANKUNFTSZEIT_GERUNDED_STUNDE'].unique()
        subplot_ax.set_xticklabels(labels)
        subplot_ax.set_xlabel('Arrival Time according to Timetable')
        subplot_ax.set_ylabel('Delay in Minutes')
        subplot_ax.set_ylim(bottom=0)

    fig.text(0.9, 0.04, 'Data source: https://opentransportdata.swiss/de/dataset/istdaten/',
             horizontalalignment='right',
             verticalalignment='bottom',
             fontsize=8)

    plt.subplots_adjust(wspace=0.25, hspace=0.5)
    plt.suptitle(
        f"STI Bus delays at 'Bahnhof Thun' station \n Route {route} ({start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')})",
        fontsize=15)

    img_data = io.BytesIO()
    fig.savefig(img_data, format='png')
    plt.close(fig)
    img_data.seek(0)

    file_path = generate_filepath(output_path, df['BETRIEBSTAG'].iloc[0], output_file_name, route)
    with fsspec.open(file_path, 'wb') as f:
        f.write(img_data.getbuffer())
    return file_path


def generate_filepath(path, timestamp, file_name, route):
    year = timestamp.strftime('%Y')
    year_month = timestamp.strftime('%Y-%m')
    return f"{path}/route-{route}/{year}/{year_month}_route-{route}_{file_name}.png"
