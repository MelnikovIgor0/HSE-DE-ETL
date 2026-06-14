import uuid
from random import randint, random, choice

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import to_json, col, struct

def generate_item():
    return (
        str(uuid.uuid4()),
        10 ** (random() * 5),
        randint(1, 20),
        choice(['food', 'medicine', 'entertainment', 'transport', 'other', 'supermarket', 'travel', 'services']),
        choice([1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 5, 10])
    )

def main():
    spark = SparkSession.builder.appName("dataproc-kafka-write-app").getOrCreate()
    schema = StructType([
        StructField('id', StringType(), True),
        StructField('price', DoubleType(), True),
        StructField('amount', IntegerType(), True),
        StructField('category', StringType(), True),
        StructField('cashback_pct', IntegerType(), True),
    ])
    items = [generate_item() for _ in range(300000)]
    df = spark.createDataFrame(items, schema)
    df = df.withColumn(
        'cashback_info',
        struct(
            col('category'),
            col('cashback_pct')
        )
    ).drop('category', 'cashback_pct')
    df = df.select(to_json(struct([col(c).alias(c) for c in df.columns])).alias('value'))
    df.write.format("kafka") \
        .option("kafka.bootstrap.servers", "rc1a-k43jgucqbhpo5181.mdb.yandexcloud.net:9091") \
        .option("topic", "dataproc-kafka-topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=user1 "
                "password=password1 "
                ";") \
        .save()

if __name__ == "__main__":
    main()
