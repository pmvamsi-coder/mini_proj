from utils.util import *


class Config:
    def __init__(self, project_name: str, path):
        data = StorageUtil(project_name).get_json(path)
        self.executor_mem = data['spark_properties']['spark.executor.memory']
        self.driver_mem = data['spark_properties']['spark.driver.memory']
        self.session_name = data['session_name']
        self.project_name = data['project_name']
        self.spark_properties = data['spark_properties']
        self.raw_file_path = data['raw_file_path']
