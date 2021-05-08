import json

from pyspark.sql import SparkSession


class Spark:
    def __init__(self, session_name: str, spark_properties: dict, project_name: str):
        self.spark = SparkSession.builder.appName(session_name).getOrCreate()
        [self.spark.conf.set(k, v) for k, v in spark_properties.items()]
        self.spark_properties = spark_properties
        self.project_name = project_name

    def get_spark(self):
        return self.spark


class StorageUtil:
    def __init__(self, project_name: str):
        self.project_name = project_name

    def get_json(self, path: str):
        with open(path, 'r') as j:
            contents = json.loads(j.read())
        return contents
