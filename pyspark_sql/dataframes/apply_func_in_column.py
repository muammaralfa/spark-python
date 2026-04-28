from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = (
        SparkSession.builder
        .appName("hello spark")
        .master("local[2]")
        .getOrCreate()
    )

    datas = [
        {
            "name": "alfa",
            "age": 21,
            "date": "2024-01-01",
            "created_at": "2025-02-15 00:00:00"
        },
        {
            "name": "baim",
            "age": 8,
            "date": "2024-02-02",
            "created_at": "2025-02-15 00:00:00"
        },
        {
            "name": "pak junaedy 2",
            "age": 43,
            "date": "2024-03-03",
            "created_at": "2025-02-15 00:00:00"
        }
    ]

    df = spark.createDataFrame(datas)
    df.show()
    df.printSchema()

    df_new = df.withColumn("date", to_date("date", "yyyy-MM-dd")) \
        .withColumn("created_at", to_timestamp("created_at"))\
        .withColumn("name", upper('name'))\
        .withColumn("age", round('age'))
    df_new.show()
    df_new.printSchema()