from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when
from pyspark.sql.functions import to_timestamp, date_format, md5, concat_ws


# Create Spark session
spark = SparkSession.builder \
    .appName("Filtering data global food") \
    .getOrCreate()

# -----------------------------
# 1. EXTRACT DATA / READ CSV
# -----------------------------
df = spark.read.csv(
    "global_food_security_intelligence.csv",
    header=True,
    inferSchema=True
)

# -----------------------------
# 2. TRANSFORM / MAPPING
# -----------------------------

""" filtering data """
df = df.filter(
    (col("country_name").isin("Aruba", "Afghanistan")) &
    (col("year") > 2010) &
    (col("longitude").isNotNull()) &
    (col("latitude").isNotNull()) &
    ((col("source_fao") == "FAOSTAT Bulk Download") | (col("source_fao") == "FAOSTAT API"))
)

""" create field id """
df = df.withColumn(
    "id",
    md5(concat_ws("||", col("country_code"), col("year")))
)

""" create new column category """
df = df.withColumn(
    "category",
    when(col("fao_undernourishment_pct").isNull(), None)
    .when(col("fao_undernourishment_pct") < 20.0, "Low")
    .when(col("fao_undernourishment_pct").between(20.0, 23.0), "Medium")
    .otherwise("High")
)

""" formating date column """
df = df.withColumn(
    "compiled_date",
    to_timestamp(
        col("compiled_date"), 
        "yyyy-MM-dd HH:mm:ss"
    )
)

df.show(1)

# -----------------------------
# 2. LOAD DATA TO POSTGRESQL
# -----------------------------

df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://192.168.114.31:5432/datas") \
    .option("dbtable", "test_spark_table_filtered") \
    .option("user", "admin") \
    .option("password", "rahasia2025") \
    .option("driver", "org.postgresql.Driver") \
    .option("batchsize", 1000)\
    .mode("overwrite") \
    .save()


# Stop Spark
spark.stop()
