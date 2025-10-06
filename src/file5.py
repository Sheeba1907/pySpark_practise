import os
import sys
os.environ['PYSPARK_PYTHON'] = r'C:\Users\sheeb\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\sheeb\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder\
        .appName("Spark func1")\
        .master("local[*]")\
        .getOrCreate()

data = [
    ("Alice", "Math", 85, 1),
    ("Bob", "Math", 90, 2),
    ("Charlie", "Math", 90, 3),
    ("David", "Math", 95, 4),
    ("Eve", "Science", 100, 1),
    ("Frank", "Science", 85, 2),
    ("Grace", "Science", 95, 3),
    ("Heidi", "Science", 90, 4),
    ("Ivan", "English", 70, 1),
    ("Judy", "English", 75, 2),
    ("Kevin", "English", 80, 3),
    ("Laura", "English", 80, 4)
]
columns = ["name", "subject", "score", "exam_no"]
df = spark.createDataFrame(data, columns)
df.show()

#Window functions
#window
WindowSpec = Window.orderBy(df["score"].desc())
WindowSpecs = Window.partitionBy(df["subject"]).orderBy(df["score"].desc())

#Row_number
df.withColumn("Row number", row_number().over(WindowSpecs)).show()
#rank
df.withColumn("Rank", rank().over(WindowSpecs)).show()
#dense_rank
df.withColumn("Dense rank", dense_rank().over(WindowSpecs)).show()
#lag
df.withColumn("Lag", lag("score", 1).over(WindowSpecs)).show()
#lead
df.withColumn("Lead", lead("score", 1).over(WindowSpecs)).show()
#cume_dist
df.withColumn("Cumelative distribution", cume_dist().over(WindowSpec)).show()
#percent_rank
df.withColumn("Percentage rank", percent_rank().over(WindowSpec)).show()

#Array functions
data = [("Alice", 85, 90, 95),
        ("Bob", 70, 80, 75),
        ("Charlie", 75, 85, 70),
        ("David", 80, 95, 85),
        ("Eve", 60, 70, 80)]
columns = ["name", "math", "science", "english"]

dff = spark.createDataFrame(data, columns)
dff.show()

#array, array_contains
dff = dff.withColumn("scores", array("math", "science", "english"))
dff = dff.withColumn("Has 80", array_contains(col("scores"), lit(80)))
dff.show()
#array_remove, array_length
dff = dff.withColumn("Remove_70", array_remove(col("scores"), 70))
dff = dff.withColumn("Length", array_size(col("scores")))
dff.show()
#array_position, concat
dff = dff.withColumn("70 position", array_position(col("scores"), 80))
dff = dff.withColumn("Concat array", concat(col("scores"), array(lit(100), lit(98))))
dff.show()
#array_distinct, sort_array, transform
dff = dff.withColumn("Distinct array", array_distinct(col("scores")))
dff = dff.withColumn("Sorted array", sort_array(col("scores")))
dff = dff.withColumn("Transformation", transform(col("scores"), lambda x: x + 10))
dff.show(truncate = False)