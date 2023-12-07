"""
1. Яка «середня» тривалість поїздки на день?
2. Скільки поїздок було здійснено кожного дня?
3. Яка була найпопулярніша початкова станція для кожного місяця?
4. Які станції входять у трійку лідерів станцій для поїздок кожного дня протягом останніх двох тижнів?
5. Чоловіки чи жінки їздять довше в середньому?
6. Який вік людей входить у десятку лідерів, хто подорожує найдовше та найкоротше?
"""

from pyspark.sql import SparkSession
import zipfile
import glob
import os
import shutil

data_dir = 'data'
temp_dir = 'temp'
archive_type = 'zip'
file_type = 'csv'
reports_dir = 'reports'

def find_files(dir, type):
    return glob.glob(dir + '/**/*.' + type, recursive=True)

def unzip(filename):
    with zipfile.ZipFile(filename, 'r') as zip_file:
        zip_file.extractall(temp_dir)
    #os.remove(filename)
    local_filename = filename.split('/')[-1]
    print(f"Архів {local_filename} разархівовано")

def clear(dir):
    if os.path.isdir(dir):
        shutil.rmtree(dir)
        print(f"Папку {dir} видалено")

def mkdir(dir):
        if not os.path.exists(dir):
            os.makedirs(dir)
            print(f"Папку {dir} створено")

def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    # your code here
    clear(temp_dir)
    mkdir(temp_dir)
    mkdir(reports_dir)
    file_list = find_files(data_dir, archive_type)
    print(file_list)
    for file_name in file_list:
        unzip(file_name)
    file_list = find_files(temp_dir, file_type)
    print(file_list)

    df = []
    for file in file_list:
        df.append(spark.read.option("header", True).csv(file))
        df[-1].show()
    #df.printSchema()


    #clear(temp_dir)

if __name__ == "__main__":
    main()
