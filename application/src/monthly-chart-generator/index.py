import os
import pandas as pd
import calendar
import json
import s3fs
import io
import matplotlib.pyplot as plt

DATA_BUCKET_NAME = os.environ['DATA_BUCKET_NAME']

def read_csv(path, csv_file_path):
    df = pd.read_csv(f"{path}{csv_file_path}",
                     sep=";",
                     dtype = {'BETRIEBSTAG': str,
                              'LINIEN_TEXT': str,
                              'ANKUNFTSZEIT': str,
                              'AN_VERSPAETUNG_MIN': float,
                              }
                     )

    df['BETRIEBSTAG'] = pd.to_datetime(df['BETRIEBSTAG'])
    df['ANKUNFTSZEIT'] = pd.to_datetime(df['ANKUNFTSZEIT'])
    df['ANKUNFTSZEIT_TIME'] = pd.to_datetime(df['ANKUNFTSZEIT']).dt.floor(freq='60min').dt.time
    df['WOCHENTAG'] = pd.to_datetime(df['ANKUNFTSZEIT']).dt.strftime('%a')
    df['WOCHENTAG_NUMMER'] = pd.to_datetime(df['ANKUNFTSZEIT']).dt.strftime('%u')
    return df

def read_csv_files(path, csv_file_path_list):
    data=[]
    for file in csv_file_path_list:
        data.append(read_csv(path, file))

    return pd.concat(data)

def generate_chart(df, line, title, path):
    linie1 = df.query(f"LINIEN_TEXT == '{line}'").sort_values(by=['WOCHENTAG_NUMMER'], ascending=True)

    ax = linie1.pivot_table(index=['FAHRT_BEZEICHNER'],
                            columns=['WOCHENTAG_NUMMER','ANKUNFTSZEIT_TIME'],
                            values=['AN_VERSPAETUNG_MIN']).plot.box(
        figsize=(20, 8),
        title=f"Verspätungen der STI Busse am Bahnhof Thun - Linie {line}\n {title}"
    )

    labels = [item.get_text().replace("(","").replace(")","").split(",")[2] for item in ax.get_xticklabels()]
    day = 0
    for index, item in enumerate(ax.get_xticklabels()):
        current_day = item.get_text().replace("(","").replace(")","").split(",")[1]
        if day != current_day:
            ax.axvline(x=(index+0.5))
            ax.text((index+0.75), 58, f"{calendar.day_name[int(current_day)-1]}", rotation='horizontal')
            day = current_day

    ax.set_xticklabels(labels)
    ax.tick_params(axis='x', labelrotation=90)
    ax.set_ybound(lower=0, upper=60)

    fig = ax.get_figure()
    img_data = io.BytesIO()
    fig.savefig(img_data, format='png')
    plt.close(fig)
    img_data.seek(0)

    year = df['BETRIEBSTAG'].dt.year.values[0]
    month = df['BETRIEBSTAG'].dt.month.values[0]

    s3 = s3fs.S3FileSystem(anon=False)  # Uses default credentials
    with s3.open(f"{path}{year}/{month}_{line}_sti_thun_bahnhof.png", 'wb') as f:
        f.write(img_data.getbuffer())

def generate_chart_for_all_lines(df, title, path):
    for line in df['LINIEN_TEXT'].unique():
        generate_chart(df, line, title, path)

def handler(event, context):
    print(event)

    bucket_name = event["detail"]["bucket"]["name"]
    folder = event["detail"]["bucket"]["name"]
    year = event["detail"]["object"]["key"].split("/")[1]
    month = event["detail"]["object"]["key"].split("/")[2]

    s3 = s3fs.S3FileSystem(anon=False)  # Uses default credentials
    files = s3.glob(f"s3://{bucket_name}/actual-data/{year}/{month}/*.csv")

    print(f"s3://{bucket_name}/actual-data/{year}/{month}/*.csv")
    print(files)

    df = read_csv_files(f"s3://",files)
    generate_chart_for_all_lines(df, "blubr", f"s3://{bucket_name}/monthly-charts/")

    # for record in event['Records']:
    #     print(record["s3"]["bucket"]["name"])
    #     print(record["s3"]["object"]["key"])
    #
    #     bucket_name = record["s3"]["bucket"]["name"]
    #     year = record["s3"]["object"]["key"].split("/")[1]
    #     month = record["s3"]["object"]["key"].split("/")[2]
    #
    #     s3 = s3fs.S3FileSystem(anon=False)  # Uses default credentials
    #     files = s3.find(f"s3://{bucket_name}/actual-data/{year}/{month}", withdirs=True)
    #
    #     print(f"s3://{bucket_name}/actual-data/{year}/{month}")
    #     print(files)
    #
    #     df = read_csv_files(f"s3://{bucket_name}/",files)
    #     generate_chart_for_all_lines(df, "blubr", f"s3://{bucket_name}/monthly-charts/")

    return {
        'statusCode': 200,
        'body': json.dumps('Generated monthly chart.')
    }

