import requests
import pandas as pd
from bs4 import BeautifulSoup
import csv
from google.cloud import storage
from datetime import datetime


def get_file_info():
    """Web scraping to get last update of each file
    """
    url = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"
    html = requests.get(url)
    soup = BeautifulSoup(html.text, "lxml")

    table = soup.find({'table': 'indexlist'})
    list_table = table.find_all('tr')

    file_info = []
    for i in range(4, len(list_table)-1):
        file = list_table[i].text[:24]
        last_modified = list_table[i].text[24:34]
        file_array = [file, last_modified]
        file_info.append(file_array)

    df_file_info = pd.DataFrame(file_info)
    df_file_info[1] =  pd.to_datetime(df_file_info[1], format='%Y-%m-%d')
    return df_file_info


def check_if_last_update_info_exists(storage_client, bucket):
    """If file containing last update info not exists, create it with 2017-01-01
    """
    filename = 'raw/data/csv/last_update.txt'
    stats = storage.Blob(bucket=bucket, name=filename).exists(storage_client)
    if not stats:
        blob = bucket.blob(filename)
        blob.upload_from_string('2017-01-01', content_type='txt')


def get_last_update(bucket):
    filename = 'raw/data/csv/last_update.txt'
    blob = bucket.get_blob(filename)
    last_update = blob.download_as_text()
    dict_last_update = {'last_update': last_update}
    df_last_update = pd.DataFrame(dict_last_update, index=[0])
    df_last_update['last_update'] =  pd.to_datetime(df_last_update['last_update'], format='%Y-%m-%d')
    return df_last_update
    

def get_updated_files(df_file_info, df_last_update):
    """Get only filenames updated since last database update
    """
    updated_files = df_file_info[df_file_info[1] >= df_last_update.iloc[0][0]].values.tolist()
    return updated_files


def upload_updated_files(updated_files, bucket):
    """Download each file and save it to gs
    """
    for file in updated_files:
        url = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/' + file[0]
        with requests.Session() as s:
            downloaded_file = s.get(url)
            decoded_content = downloaded_file.content.decode('utf-8')
            filename = 'raw/data/csv/' + file[0]
            blob = bucket.blob(filename)
            blob.upload_from_string(decoded_content, content_type='csv')


def update_last_update(bucket):
    filename = 'raw/data/csv/last_update.txt'
    blob = bucket.blob(filename)
    blob.upload_from_string(datetime.today().strftime('%Y-%m-%d'), content_type='txt')


def main(request):
    BUCKET_NAME = 'fundos-de-investimento'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET_NAME)

    df_file_info = get_file_info()
    check_if_last_update_info_exists(storage_client, bucket)
    df_last_update = get_last_update(bucket)

    updated_files = get_updated_files(df_file_info, df_last_update)
    upload_updated_files(updated_files, bucket)

    update_last_update(bucket)

    return f'success'
