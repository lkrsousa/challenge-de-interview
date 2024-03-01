from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, count, desc, asc
from equipment import EquipmentReader
from equipment_failure import EquipmentFailureReader
from equipment_sensor import EquipmentSensorReader

spark = SparkSession.builder.appName("read_sensor_data").getOrCreate()

sc = spark.sparkContext

equipment_reader = EquipmentReader(spark)

equipments = equipment_reader.read_json("equipment.json")

equipment_failure_reader = EquipmentFailureReader(spark)

equipment_failure_data = equipment_failure_reader.read_text(
    "equipment_failure_sensors/equpment_failure_sensors.txt"
)

equipment_failure_formated = equipment_failure_reader.format_data(
    equipment_failure_data
)

equipment_sensor_reader = EquipmentSensorReader(spark)

equipment_sensors = equipment_sensor_reader.read_csv("equipment_sensors.csv")

equipment_with_failure = equipment_failure_reader.join_sensor_data(
    equipment_sensors, equipment_failure_formated
)

print("1. Total equipment failures that happened?")
equipment_failure_reader.get_total_equipment_failure(equipment_with_failure).show()


print("2. Which equipment name had most failures?")
quantity_of_fail = equipment_failure_reader.get_quantity_of_fail(equipment_with_failure)

equipment_most_failed = equipment_failure_reader.get_equipment_most_failed(
    quantity_of_fail, equipments
)

print(equipment_most_failed)

print(
    "3. Average amount of failures across equipment group, ordered by the number of failures in ascending order?"
)
total_failure_equipment = equipment_failure_reader.get_average_failures_by_group(
    quantity_of_fail, equipments
).show()

print(
    "4.  Rank the sensors which present the most number of errors by equipment name in an equipment group."
)
sensor_most_failed_by_equipment = (
    equipment_failure_reader.get_sensor_most_failed_by_equipment(
        equipment_failure_formated, equipment_sensors, equipments
    ).show()
)
