from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("calc-cashback") \
    .getOrCreate()

df = spark.read.parquet("s3a://bucket-managed-airflow/cashbacks")

df = df.withColumn(
    "cashback_amount",
    (col("cashback_pct") / 100) * (col("amount") * col("price"))
)

df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("s3a://bucket-managed-airflow/output")

spark.stop()
