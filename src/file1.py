import os
import sys
from pyspark.sql.types import *
from pyspark.sql import SparkSession
os.environ['PYSPARK_PYTHON'] = r'C:\Users\sheeb\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\sheeb\AppData\Local\Programs\Python\Python311\python.exe'

spark = SparkSession.builder\
        .appName("Spark Intro")\
        .master("local[*]")\
        .getOrCreate()

