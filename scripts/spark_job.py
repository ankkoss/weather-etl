from pyspark.sql import SparkSession

def run_spark(records):
    spark = SparkSession.builder \
        .appName("Weather ETL") \
        .getOrCreate()

    df = spark.createDataFrame(records)

    df.show(5)

    return df