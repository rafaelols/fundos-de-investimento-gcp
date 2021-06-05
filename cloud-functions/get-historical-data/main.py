from google.cloud import storage
from io import BytesIO
import requests
import zipfile


def extract_file(filebytes):
    with zipfile.ZipFile(filebytes, 'r') as myzip:
        for contentfilename in myzip.namelist():    
            contentfile = myzip.read(contentfilename)
            return contentfile


def get_file_from_each_year(year):
    url = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_'+str(year)+'.zip'
    # Content download
    filebytes = BytesIO(
        requests.get(url, stream=True).content
    )
    return filebytes


def upload_blob(bucket_name, upload_file, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    #blob.upload_from_file(upload_file, content_type='zip')
    blob.upload_from_string(upload_file, content_type='csv')

    print('File uploaded to {}.'.format(
        destination_blob_name))


def main(request):
    BUCKET_NAME = 'fundos-de-investimento'

    for year in range(2005, 2017):
        BLOB_NAME = 'historical_data/'+str(year)+'.csv'
        zip_file = get_file_from_each_year(year)
        upload_file = extract_file(zip_file)
        upload_blob(BUCKET_NAME, upload_file, BLOB_NAME)

    return f'Success!'
