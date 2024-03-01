import json
import ast

class EquipmentReader:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.sc = self.spark.sparkContext

    def read_json(self, file_path):
        equipment = (
            self.sc.wholeTextFiles(file_path)
            .map(lambda x: ast.literal_eval(x[1]))
            .map(lambda x: json.dumps(x))
        )

        equipment_df = self.spark.read.json(equipment)
        return equipment_df
