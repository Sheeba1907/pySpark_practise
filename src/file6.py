import os
import sys
os.environ['PYSPARK_PYTHON'] = r'C:\Users\sheeb\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\sheeb\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

spark = SparkSession.builder\
        .appName("Spark readWrite")\
        .master("local[*]")\
        .getOrCreate()
#Reading and Writing
#Reading csv
schemas = StructType([StructField("emp_id", IntegerType(), True),
                     StructField("name", StringType(), True),
                     StructField("age", IntegerType(), True),
                     StructField("gender", StringType(), True),
                     StructField("department", StringType(), True),
                     StructField("location", StringType(), True),
                     StructField("salary", IntegerType(), True),
                     StructField("joining_date", DateType(), True)])
df_csv = spark.read.csv(r"C:\Users\sheeb\OneDrive\Documents\employees.csv", header=True, schema=schemas)
df_csv.show()

#Reading parquet
df_parquet = spark.read.parquet(r"C:\Users\sheeb\Downloads\mtcars.parquet")
df_parquet.show()

"""schemas1 = StructType([StructField("Model", StringType(), True),
                     StructField("Miles per gallon", IntegerType(), True),
                     StructField("No. of cylinders", IntegerType(), True),
                     StructField("Displacement", FloatType(), True),
                     StructField("Horse power", IntegerType(), True),
                     StructField("Rear axle ratio", FloatType(), True),
                     StructField("Weight", FloatType(), True),
                     StructField("Mile time", FloatType(), True),
                     StructField("Engine type", IntegerType(), True),
                     StructField("Transmission", IntegerType(), True),
                     StructField("Gear", IntegerType(), True),
                     StructField("Carburetors", IntegerType(), True) ])"""
#dff_parquet = spark.read.schema(schemas1).parquet(r"C:\Users\sheeb\Downloads\mtcars.parquet")
#dff_parquet.show()

#Reading JSON
schemas1 = StructType([StructField("emp_id", IntegerType(), True),
                     StructField("name", StringType(), True),
                     StructField("age", IntegerType(), True),
                     StructField("gender", StringType(), True),
                     StructField("department", StringType(), True),
                     StructField("location", StringType(), True),
                     StructField("salary", IntegerType(), True),
                     StructField("joining_date", DateType(), True)])
df_json = spark.read.json(r"C:\Users\sheeb\OneDrive\Documents\employees.json", schema=schemas1)
df_json.show()