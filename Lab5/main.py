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
from pyspark.sql.types import IntegerType

from pyspark.sql.functions import *

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

#def duration_udf(start_time, end_time):
#    a = udf((timestamp_seconds(end_time) - timestamp_seconds(start_time)), IntegerType())
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
        #df[-1].show()
        #df[-1].printSchema()

    #df[0].groupby('from_station_name').avg().show()
    ##df[0].select(col(start_time) 'date', df[0].start_time).show()



    #df[0].withColumn("date", to_date(col("start_time")))
    df[0] = df[0].drop('trip_id', 'bikeid', 'tripduration', 'from_station_id', 'to_station_id', 'usertype')
    df[1] = df[1].drop('ride_id', 'rideable_type', 'start_station_id', 'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'member_casual')

    df[0] = df[0].withColumnRenamed("from_station_name", "start_station_name")
    df[0] = df[0].withColumnRenamed("to_station_name", "end_station_name")
    df[1] = df[1].withColumnRenamed("started_at", "start_time")
    df[1] = df[1].withColumnRenamed("ended_at", "end_time")

    df[0].show()
    df[1].show()

    moment_year = 2020
    df[0] = df[0].withColumn('age', moment_year - col('birthyear'))
    df[0] = df[0].drop('birthyear') #.show()

    #sorted(df[0].groupBy('sex').agg({'age': 'mean'}).collect())
    #df[0].groupby('from_station_name').avg().show()

    merged_df = df[1].union(df[0].drop('age', 'gender'))
    merged_df.show()

    #df[1].withColumn('tripduration', (timestamp_seconds(col("end_time")) - timestamp_seconds(col("start_time")))).show()
    #df[1].withColumn('tripduration', (timestamp_seconds(col("start_time")) - timestamp_seconds(col("end_time")))).show()
    #df[1].withColumn('tripduration', duration_udf(col("start_time"), col("end_time"))).show()
    #df[1].withColumn('tripduration', col("ended_at")).show()
    #df[1].withColumnRenamed("ended_at", "end_time").show()

"""
    df[0].select(col("start_time"),
            to_date(col("start_time")).alias("date"),
            to_date(col("end_time")).alias("end date"),
            #).groupby('date').avg().show()
            ).groupby('date').avg().show()
"""
    #clear(temp_dir)

if __name__ == "__main__":
    main()
