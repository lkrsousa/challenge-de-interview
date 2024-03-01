import json
import ast


class Equipment:
    def __init__(self, equipment_id, sensor_id):
        self.equipment_id = equipment_id
        self.sensor_id = sensor_id


class EquipmentSensorReader:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.sc = self.spark.sparkContext

    def read_csv(self, file_path):

        result = self.spark.read.option("header", True).csv(file_path)
        return result
