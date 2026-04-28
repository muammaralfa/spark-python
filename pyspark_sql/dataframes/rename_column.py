from pyspark.sql import SparkSession


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
            "age": 21
        },
        {
            "name": "baim",
            "age": 8
        },
        {
            "name": "pak junaedy 2",
            "age": 43
        }
    ]

    df = spark.createDataFrame(datas)
    renamed_df = df.withColumnRenamed("name", "short_name")
    renamed_df.show()