#import pyspark as sp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import StringType,ArrayType
import os #,sys
from re import sub

#os.environ['PYSPARK_PYTHON'] = sys.executable
#os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.1.1'

def processing_message(commit_message:str): # Обробка тексту комітів
    # фільтруємо текст від зайвих символів
    commit_message = commit_message.lower()
    commit_message = sub("[!?.,/=+_<>{}#$'`~|*&()^%@:]", ' ', commit_message)
    commit_message = commit_message.replace('"',' ')
    commit_message = commit_message.replace('-',' ')
    #commit_message = commit_message.replace(r"\\", "")
    commit_message = commit_message.replace('[',' ')
    commit_message = commit_message.replace(']',' ')
    commit_message = commit_message.replace('   ',' ')
    commit_message = commit_message.replace('  ',' ')
    commit_message = commit_message.replace('\n',' ')
    commit_message = commit_message.replace('\t',' ')
    commit_message = commit_message.replace('\\',' ')
    commit_message = commit_message.rstrip().lstrip()

    # Ділимо текст на окремі слова, кожне з яких обрізаємо до перших трьох символів
    words = commit_message.split(" ")[:5]
    temp = []
    for word in words:
        temp.append(word[:3])
    words = temp
    collections = [words[i:i+3] for i in range(3)]

    return [" ".join(collection) for collection in collections]

# Ініціалізація сесії Spark
spark = SparkSession.builder.appName("lab1de").getOrCreate()

# Імпортуємо дані з файлу
df = spark.read.json("10K.github.jsonl")

# Фільтруємо рядки за значенням типу PushEvent
df = df.where(df['type'] == 'PushEvent')

# вибираємо з payload всі commits
df = df.select(explode("payload.commits").alias("commit"))

# вибираємо в першу колонку ім'я автора а в другу повідомлення
df = df.select("commit.author.name", "commit.message")

for i in range(3):
    # додаємо новий стовбець "3_grams_N" який буде заповнюватись за допомогою функції processing_message
    my_udf = udf(processing_message, ArrayType(StringType()))
    df = df.withColumn("3_grams_"+str(i), my_udf(col("message"))[i])

# Видаляємо зайвий стовбець
df = df.drop("message")

# Виведення схеми та декількох перших рядків
df.printSchema()
df.show()

# Записуємо результат у csv файл
df.toPandas().to_csv("output_file.csv", index=False)
#df.write.csv("output_file2.csv")
#df.write.option("header",True).csv("output_file.csv")

