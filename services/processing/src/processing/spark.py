from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()
