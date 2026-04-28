from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType
from pyspark.sql.functions import *
import os


POSTGRES_URL = "jdbc:postgresql://10.100.1.181:5433/postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "rahasia"
POSTGRES_TABLE = "imbd_movies"


def lowercase_column_names(df):
    new_columns = [col_name.lower() for col_name in df.columns]
    df_lower = df.toDF(*new_columns)
    return df_lower


""" define spark session. set config connector apa aja yg dipake, contoh dibawah postgre """
spark = SparkSession.builder \
    .master("local[2]") \
    .appName("elt_csv_to_pg") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4") \
    .config("spark.driver.extraClassPath", "/resources/postgresql-42.7.4.jar") \
    .getOrCreate()


schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("startYear", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("averageRating", FloatType(), True),
    StructField("numVotes", IntegerType(), True),
    StructField("runtimeMinutes", IntegerType(), True),
    StructField("directors", StringType(), True),
    StructField("writers", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("IMDbLink", StringType(), True),
    StructField("Title_IMDb_Link", StringType(), True)
])

"""  
    READ USING SPARK DATAFRAME
    inferSchema = False untuk mapping data format sesuai mapping yang kita inginkan 
    inferSchema = True kalau ingin type data sesuai file, gausah pake param schema
"""
df = spark.read.csv(
    "../resources/results_with_crew.csv",
    header=True,
    schema=schema,
    inferSchema=False
)

""" transform data. disini rename column, upper value directors dan lower all column name"""
df = df.withColumnRenamed("tconst", "id")
df = df.withColumn("directors", upper("directors"))
df = lowercase_column_names(df=df).cache()
df.show()

"""   Save to Parquet  """
# df.write.parquet(PARQUET_OUTPUT_PATH)

"""  load into postgre  """
df.write \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", POSTGRES_TABLE) \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .option("batchsize", 1000) \
    .mode("overwrite") \
    .save()

print("Data successfully load into postgre")
spark.stop()
