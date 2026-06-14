from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

def main():
    spark = SparkSession.builder.appName("dataproc-kafka-read-batch-app").getOrCreate()
    schema = StructType([
        StructField('id', StringType(), True),
        StructField('price', DoubleType(), True),
        StructField('amount', IntegerType(), True),
        StructField('cashback_info', StructType([
            StructField('category', StringType(), True),
            StructField('cashback_pct', IntegerType(), True),
        ]), True),
    ])
    df = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", "rc1a-k43jgucqbhpo5181.mdb.yandexcloud.net:9091") \
        .option("subscribe", "dataproc-kafka-topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=user1 "
                "password=password1 "
                ";") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .where(col("value").isNotNull()) \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .select(
            col("id"),
            col("price"),
            col("amount"),
            col("cashback_info.category").alias("category"),
            col("cashback_info.cashback_pct").alias("cashback_pct")
        )
    df.write.mode("overwrite").json("s3a://dataproc-bucket/kafka-read-batch-output")

if __name__ == "__main__":
    main()
