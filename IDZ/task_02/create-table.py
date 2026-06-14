import uuid
from random import randint, random, choice

from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("create-cashbacks-table") \
    .getOrCreate()

schema = StructType([
    StructField('id', StringType(), True),
    StructField('price', DoubleType(), True),
    StructField('amount', IntegerType(), True),
    StructField('category', StringType(), True),
    StructField('cashback_pct', IntegerType(), True),
])

def generate_item():
    return (
        str(uuid.uuid4()),
        10 ** (random() * 5),
        randint(1, 20),
        choice(['food', 'medicine', 'entertainment', 'transport', 'other', 'supermarket', 'travel', 'services']),
        choice([1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 5, 10])
    )

items = [generate_item() for _ in range(700000)]

df = spark.createDataFrame(items, schema)

df.write.mode("overwrite").parquet("s3a://bucket-managed-airflow/cashbacks")

spark.stop()
