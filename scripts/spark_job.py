from pyspark.sql import SparkSession
import os

def run_spark(records):
    print("SPARK STARTED")

    os.environ["PYSPARK_PYTHON"] = "C:\\Python310\\python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Python310\\python.exe"
    
    spark = SparkSession.builder \
        .appName("Weather ETL") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = spark.createDataFrame(records)

    print("SHOW")
    df.show(5)

    print("SCHEMA")
    df.printSchema()

    print("DESCRIBE")
    df.describe().show()

    print("GROUP BY")
    df.groupBy("city").avg("temperature").show()

    return df