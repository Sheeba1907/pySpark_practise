import os
import sys
from pyspark.sql.types import *
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = r'C:\Users\sheeb\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\sheeb\AppData\Local\Programs\Python\Python311\python.exe'

spark = SparkSession.builder \
    .appName("Spark Intro") \
    .master("local[*]") \
    .getOrCreate()

empData = [
    [101, "John", "Doe", "Sales", 55000, "2018-04-23", 201],
    [102, "Jane", "Smith", "Finance", 62000, "2019-06-12", 202],
    [103, "Michael", "Brown", "IT", 72000, "2017-09-30", 203],
    [104, "Linda", "Johnson", "HR", 50000, "2020-01-15", 201],
    [105, "David", "Williams", "Marketing", 58000, "2018-11-05", 202],
    [106, "Susan", "Jones", "IT", 75000, "2016-07-19", 203],
    [107, "Robert", "Miller", "Finance", 67000, "2019-03-25", 202],
    [108, "Karen", "Davis", "Sales", 53000, "2021-02-10", 201],
    [109, "James", "Wilson", "IT", 80000, "2015-05-14", 203],
    [110, "Patricia", "Taylor", "HR", 52000, "2020-09-01", 201]]

from pyspark.sql.types import *

empSchema = StructType([StructField(name="empid", dataType=IntegerType()),
                        StructField(name="firstName", dataType=StringType()),
                        StructField(name="lastName", dataType=StringType()),
                        StructField(name="department", dataType=StringType()),
                        StructField(name="salary", dataType=IntegerType()),
                        StructField(name="hireDate", dataType=StringType()),
                        StructField(name="managerID", dataType=IntegerType())
                        ])

df = spark.createDataFrame(data=empData, schema=empSchema)

df.rdd.getNumPartitions()

#df.show()

df1 = df.where("salary > 70000")
#df1.show()

df2 = df.select("firstName", "department")
#df2.show()

df3 = df.filter(df.department == "IT")
#df3.show()

df4 = df.groupBy("department").avg("salary")
#df4.show()

from pyspark.sql.functions import round

df5 = df4.withColumn("avg(salary)", round("avg(salary)", 2))
#df5.show()

from pyspark.sql.functions import col

df6 = df.withColumn("bonus", col("salary") * 0.1)
#df6.show()

from pyspark.sql.functions import lit

df7 = df.withColumn("company", lit("ABC Corporation Ltd"))
#df7.show()

from pyspark.sql.functions import lit

df8 = df.withColumn("Code", lit(123456))
#df8.show()

df9 = df8.drop("Code")
#df9.show()

df7.createOrReplaceTempView("Employees")
spark.sql("select department, avg(salary) as Average from Employees group by department, managerID").show()

df10 = spark.read.csv(r"C:\Users\sheeb\Downloads\archive\Sales.csv", header=True, inferSchema=True)
df10.show()
