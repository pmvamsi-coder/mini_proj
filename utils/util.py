import json

from pyspark.sql import SparkSession


class Spark:
    def __init__(self, session_name: str, spark_properties: dict, project_name: str):
        self.spark = SparkSession.builder.appName(session_name).getOrCreate()
        [self.spark.conf.set(k, v) for k, v in spark_properties.items()]
        self.spark_properties = spark_properties
        self.project_name = project_name


def get_json():
    config = json.loads('../config/config.json')
    return config
