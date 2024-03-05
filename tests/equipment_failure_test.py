import pytest
from equipment_failure import EquipmentFailureReader
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql.functions import avg, max, count, desc, asc


class TestEquipmentFailureReader:

    spark = SparkSession.builder.appName("read_sensor_data").getOrCreate()
    equipment_reader = EquipmentFailureReader(spark)

    def test_read_json(self):
        file_path = "tests/equpment_failure_sensors.txt"
        assert isinstance(self.equipment_reader.read_text(file_path), DataFrame)

    def test_format_data(self):

        mock_formated = self.spark.createDataFrame(
            [("2021-05-18 0:20:48", "ERROR", 5820, 311.29, 6749.50)],
            T.StructType(
                [
                    T.StructField("timestamp", T.StringType(), True),
                    T.StructField("level", T.StringType(), True),
                    T.StructField("sensor_id", T.IntegerType(), True),
                    T.StructField("temperature", T.FloatType(), True),
                    T.StructField("vibration", T.FloatType(), True),
                ]
            ),
        )

        data_list = [
            "[2021-05-18 0:20:48]\tERROR\tsensor[5820]:\t(temperature\t311.29, vibration\t6749.50)",
            "[2021-05-18 0:20:48]\tERROR\tsensor[5820]:\t(temperature\t311.29, vibration\t6749.50)",
            "[2021-06-14 19:46:9]\tERROR\tsensor[3359]:\t(temperature\t270.00, vibration\t-335.39)",
            "[2020-09-27 22:55:11]\tERROR\tsensor[9503]:\t(temperature\t255.84, vibration\t1264.54)",
            "[2019-02-9 20:56:4]\tERROR\tsensor[3437]:\t(temperature\t466.57, vibration\t-1865.26)",
        ]
        data = self.spark.createDataFrame(data_list, "string").toDF("value")

        data_formated = self.equipment_reader.format_data(data)

        assert mock_formated.schema == data_formated.schema

    def test_join_sensor_data(self):
        equip_failure = self.spark.createDataFrame(
            [("2021-05-18 0:20:48", "ERROR", 5820, 311.29, 6749.50)],
            T.StructType(
                [
                    T.StructField("timestamp", T.StringType(), True),
                    T.StructField("level", T.StringType(), True),
                    T.StructField("sensor_id", T.IntegerType(), True),
                    T.StructField("temperature", T.FloatType(), True),
                    T.StructField("vibration", T.FloatType(), True),
                ]
            ),
        )

        sensor_data = self.spark.createDataFrame(
            [(1, 4275)],
            T.StructType(
                [
                    T.StructField("equipment_id", T.StringType(), True),
                    T.StructField("sensor_id", T.StringType(), True),
                ]
            ),
        )

        assert isinstance(
            self.equipment_reader.join_sensor_data(sensor_data, equip_failure),
            DataFrame,
        )

    def test_total_equipment_failure(self):
        equip_failure = self.spark.createDataFrame(
            [
                ("2021-05-18 0:20:48", "ERROR", 5820, 311.29, 6749.50, 1, 4275),
                ("2021-05-18 0:20:48", "ERROR", 5820, 311.29, 6749.50, 2, 4276),
            ],
            T.StructType(
                [
                    T.StructField("timestamp", T.StringType(), True),
                    T.StructField("level", T.StringType(), True),
                    T.StructField("sensor_id", T.IntegerType(), True),
                    T.StructField("temperature", T.FloatType(), True),
                    T.StructField("vibration", T.FloatType(), True),
                    T.StructField("equipment_id", T.StringType(), True),
                    T.StructField("sensor_id", T.StringType(), True),
                ]
            ),
        )

        total = self.spark.createDataFrame(
            [
                (2,),
            ],
            T.StructType(
                [
                    T.StructField("total_failure", T.LongType(), False),
                ]
            ),
        )

        data = self.equipment_reader.get_total_equipment_failure(equip_failure)

        assert data.collect() == total.collect()

    def test_get_quantity_of_fail(self):
        equip_failure = self.spark.createDataFrame(
            [
                ("2021-05-18 0:20:48", "ERROR", 5820, 311.29, 6749.50, 1, 4275),
                ("2021-05-18 0:20:48", "ERROR", 5820, 311.29, 6749.50, 2, 4276),
            ],
            T.StructType(
                [
                    T.StructField("timestamp", T.StringType(), True),
                    T.StructField("level", T.StringType(), True),
                    T.StructField("sensor_id", T.IntegerType(), True),
                    T.StructField("temperature", T.FloatType(), True),
                    T.StructField("vibration", T.FloatType(), True),
                    T.StructField("equipment_id", T.StringType(), True),
                    T.StructField("sensor_id", T.StringType(), True),
                ]
            ),
        )

        expected_result = self.spark.createDataFrame(
            [
                (1, 1),
                (2, 1),
            ],
            T.StructType(
                [
                    T.StructField("equipment_id", T.StringType(), True),
                    T.StructField("count_failure", T.LongType(), True),
                ]
            ),
        )

        result = self.equipment_reader.get_quantity_of_fail(equip_failure)

        assert result.collect() == expected_result.collect()

    def test_get_equipment_most_failed(self):

        equipments = self.spark.createDataFrame(
            [
                ("1", "FGHQWR2Q", "5310B9D7"),
                ("2", "VAPQY59S", "43B81579"),
            ],
            T.StructType(
                [
                    T.StructField("equipment_id", T.StringType(), True),
                    T.StructField("group_name", T.StringType(), True),
                    T.StructField("name", T.StringType(), True),
                ]
            ),
        )

        quantity_of_fail = self.spark.createDataFrame(
            [
                (1, 1),
                (2, 5),
            ],
            T.StructType(
                [
                    T.StructField("equipment_id", T.StringType(), True),
                    T.StructField("count_failure", T.LongType(), True),
                ]
            ),
        )

        result = self.equipment_reader.get_equipment_most_failed(
            quantity_of_fail, equipments
        )

        assert result == "43B81579"

    def test_get_average_failures_by_group(self):

        expected_result = self.spark.createDataFrame(
            [
                ("FGHQWR2Q", 1.0),
                ("VAPQY59S", 5.0),
            ],
            T.StructType(
                [
                    T.StructField("group_name", T.StringType(), True),
                    T.StructField("avg(count_failure)", T.DoubleType(), True),
                ]
            ),
        )

        equipments = self.spark.createDataFrame(
            [
                ("1", "FGHQWR2Q", "5310B9D7"),
                ("2", "VAPQY59S", "43B81579"),
            ],
            T.StructType(
                [
                    T.StructField("equipment_id", T.StringType(), True),
                    T.StructField("group_name", T.StringType(), True),
                    T.StructField("name", T.StringType(), True),
                ]
            ),
        )

        quantity_of_fail = self.spark.createDataFrame(
            [
                (1, 1),
                (2, 5),
            ],
            T.StructType(
                [
                    T.StructField("equipment_id", T.StringType(), True),
                    T.StructField("count_failure", T.LongType(), True),
                ]
            ),
        )

        result = self.equipment_reader.get_average_failures_by_group(
            quantity_of_fail, equipments
        )

        assert result.collect() == expected_result.collect()

    def test_get_sensor_most_failed_by_equipment(self):

        expected_result = self.spark.createDataFrame(
            [
                ("5310B9D7", "FGHQWR2Q", 1),
            ],
            T.StructType(
                [
                    T.StructField("name", T.StringType(), True),
                    T.StructField("group_name", T.StringType(), True),
                    T.StructField("max_count", T.LongType(), True),
                ]
            ),
        )

        equipments = self.spark.createDataFrame(
            [
                ("1", "FGHQWR2Q", "5310B9D7"),
                ("2", "VAPQY59S", "43B81579"),
            ],
            T.StructType(
                [
                    T.StructField("equipment_id", T.StringType(), True),
                    T.StructField("group_name", T.StringType(), True),
                    T.StructField("name", T.StringType(), True),
                ]
            ),
        )

        equipment_failure_formated = self.spark.createDataFrame(
            [("2021-05-18 0:20:48", "ERROR", 4275, 311.29, 6749.50)],
            T.StructType(
                [
                    T.StructField("timestamp", T.StringType(), True),
                    T.StructField("level", T.StringType(), True),
                    T.StructField("sensor_id", T.IntegerType(), True),
                    T.StructField("temperature", T.FloatType(), True),
                    T.StructField("vibration", T.FloatType(), True),
                ]
            ),
        )

        sensor_data = self.spark.createDataFrame(
            [(1, 4275)],
            T.StructType(
                [
                    T.StructField("equipment_id", T.StringType(), True),
                    T.StructField("sensor_id", T.StringType(), True),
                ]
            ),
        )

        result = self.equipment_reader.get_sensor_most_failed_by_equipment(
            equipment_failure_formated, sensor_data, equipments
        )

        assert result.collect() == expected_result.collect()
