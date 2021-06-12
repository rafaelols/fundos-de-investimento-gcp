from google.cloud import storage
from io import BytesIO
import requests
import zipfile


def get_file_from_each_year(year):
    url = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_'+str(year)+'.zip'
    # Content download
    filebytes = BytesIO(
        requests.get(url, stream=True).content
    )
    return filebytes


def extract_file(filebytes):
    with zipfile.ZipFile(filebytes, 'r') as myzip:
        for contentfilename in myzip.namelist():    
            contentfile = myzip.read(contentfilename)
            return contentfile


def upload_blob(bucket, upload_file, filename):
    """Uploads a file to the bucket."""
    blob = bucket.blob(filename)
    blob.upload_from_string(upload_file, content_type='csv')


def main(request):
    BUCKET_NAME = 'fundos-de-investimento'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET_NAME)
    
    for year in range(2005, 2017):
        filename = 'raw/historical_data/csv/'+str(year)+'.csv'
        zip_file = get_file_from_each_year(year)
        upload_file = extract_file(zip_file)
        upload_blob(bucket, upload_file, filename)

    return f'Success!'
