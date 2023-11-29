#import pyspark as sp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import StringType, ArrayType
import os,sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Ініціалізація SparkSession
spark = SparkSession.builder.appName("lab1de").getOrCreate()
# читаємо файл
df_git = spark.read.json("10K.github.jsonl")
