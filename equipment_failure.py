import json
import ast
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import avg, max, count, desc, asc


class Equipment:
    def __init__(self, timestamp, level, sensor_id, temperature, vibration):
        self.timestamp = timestamp
        self.level = level
        self.sensor_id = sensor_id
        self.temperature = temperature
        self.vibration = vibration


class EquipmentFailureReader:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.sc = self.spark.sparkContext

    def read_text(self, file_path):

        equipment_failure_df = self.spark.read.text(file_path)
        return equipment_failure_df

    def format_data(self, data):

        formated = data.select(
            regexp_extract("value", r"\[(.*?)\]", 1).alias("timestamp"),
            regexp_extract("value", r"\]\s+(\w+)", 1).alias("level"),
            regexp_extract("value", r"sensor\[(\d+)\]", 1)
            .cast("int")
            .alias("sensor_id"),
            regexp_extract("value", r"temperature\s+([-]?\d+\.\d+)", 1)
            .cast("float")
            .alias("temperature"),
            regexp_extract("value", r"vibration\s+([-]?\d+\.\d+)", 1)
            .cast("float")
            .alias("vibration"),
        )
        return formated

    def join_sensor_data(self, equipment_sensors, equipment_failure_formated):
        result = equipment_failure_formated.join(
            equipment_sensors,
            equipment_failure_formated.sensor_id == equipment_sensors.sensor_id,
        )
        return result

    def get_total_equipment_failure(self, equipment_with_failure):
        result = equipment_with_failure.select(
            countDistinct("equipment_id").alias("total_failure")
        )

        return result

    def get_quantity_of_fail(self, equipment_with_failure):
        result = equipment_with_failure.groupBy("equipment_id").agg(
            count("*").alias("count_failure")
        )
        return result

    def get_equipment_most_failed(self, quantity_of_fail, equipments):
        result = (
            quantity_of_fail.join(
                equipments, equipments.equipment_id == quantity_of_fail.equipment_id
            )
            .orderBy(desc("count_failure"))
            .first()["name"]
        )

        return result

    def get_average_failures_by_group(self, quantity_of_fail, equipments):
        total_failure_equipment = quantity_of_fail.join(
            equipments, equipments.equipment_id == quantity_of_fail.equipment_id
        )

        result = (
            total_failure_equipment.groupBy("group_name")
            .agg(avg("count_failure"))
            .orderBy(asc("avg(count_failure)"))
        )

        return result

    def get_sensor_most_failed_by_equipment(
        self, equipment_failure_formated, equipment_sensors, equipments
    ):
        sensor_failure = (
            equipment_failure_formated.where(
                equipment_failure_formated.level == "ERROR"
            )
            .groupBy("sensor_id")
            .agg(count("*").alias("_count"))
            .join(equipment_sensors, ["sensor_id"])
        )

        max_equip_failure = sensor_failure.groupBy("equipment_id").agg(
            max("_count").alias("max_count")
        )

        result = max_equip_failure.join(
            sensor_failure,
            (sensor_failure._count == max_equip_failure.max_count)
            & (sensor_failure.equipment_id == max_equip_failure.equipment_id),
        )

        return result.join(equipments, ["equipment_id"]).select(
            "name", "group_name", "max_count"
        )
