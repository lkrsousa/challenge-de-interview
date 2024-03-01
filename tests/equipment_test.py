import pytest
from equipment import EquipmentReader
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

class TestEquipmentReader:

    def test_read_json(self):
        spark = SparkSession.builder.appName("read_sensor_data").getOrCreate()
        equipment_reader = EquipmentReader(spark)
        file_path = "equipment.json"
        assert isinstance(equipment_reader.read_json(file_path), DataFrame)
