import requests
#import re
import os
import zipfile
import shutil

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

download_cat = 'downloads'

def processing_url(url):
    print(f"Processing url: {url}")
    local_filename = url.split('/')[-1]
    print("Filename:", local_filename)
    with requests.get(url, stream=True, timeout=2.50, allow_redirects=True) as r:
        content(r)
        if r.status_code != 200:
            print(f"Не вдалося завантажити файл з URL: {url}")
            return
        file_name = os.path.join(download_cat, local_filename)
        download_file(r, file_name)
        unzip(file_name)
    return local_filename

def download_file(object, file_name):
    with open(file_name, 'wb') as file:
        for chunk in object.iter_content(chunk_size=1024):
            file.write(chunk)
    local_filename = file_name.split('/')[-1]
    print(f"Завантажено: {local_filename}")

def content(object):
    content_type = object.headers.get(('content-type'))
    content_type_str = str(content_type).split('/')[-1]
    content_type_str = content_type_str.upper()
    content_length = object.headers.get('content-length', None)
    if content_length != None:
        size_mb = round(int(content_length) / 1024 / 1024, 1)
        size_str = str(size_mb) + " MB"
    else:
        size_str = None
    print(f"Type: {content_type_str} | Size: {size_str}")

def unzip(filename):
    with zipfile.ZipFile(filename, 'r') as zip_file:
        zip_file.extractall(download_cat)
    os.remove(filename)
    local_filename = filename.split('/')[-1]
    print(f"Архів {local_filename} разархівовано та видалено")

def print_results(download_cat):
    if os.path.isdir(download_cat):
        print(os.listdir(download_cat))

def crear(download_cat):
    if os.path.isdir(download_cat):
        shutil.rmtree(download_cat)
        print(f"Папку {download_cat} видалено")

def main():
    # розв'язок
    if not os.path.exists(download_cat):
        os.makedirs(download_cat)

    for url in download_uris:
        print()
        processing_url(url)

    print()
    print_results(download_cat)
    crear(download_cat)

if __name__ == "__main__":
    main()
