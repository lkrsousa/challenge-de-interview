import pytest
from equipment_sensor import EquipmentSensorReader
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class TestEquipmentSensorReader:

    def test_read_json(self):
        spark = SparkSession.builder.appName("read_sensor_data").getOrCreate()
        equipment_reader = EquipmentSensorReader(spark)
        file_path = "equipment_sensors.csv"
        assert isinstance(equipment_reader.read_csv(file_path), DataFrame)
