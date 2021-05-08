import argparse

from config.config import *


class Ingestion:

    def __init__(self, args: dict):
        self.cfg = Config(args['project_name'], args['path'])
        self.spark = Spark(self.cfg.session_name, self.cfg.spark_properties, self.cfg.project_name).spark

    def process(self):
        self.spark.read.option('header', 'true').csv(self.cfg.raw_file_path).show()
        for i in self.spark.sparkContext.getConf().getAll():
            print(i)
        self.spark.stop()


def main():
    args = {
        "project_name": "Vamsi",
        "path": "/Users/vamsi/PycharmProjects/mini/config/config.json"
    }

    ingestion_process = Ingestion(args)
    ingestion_process.process()


if __name__ == "__main__":
    main()
