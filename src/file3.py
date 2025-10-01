import os
import sys
from pyspark.sql.types import *
os.environ['PYSPARK_PYTHON'] = r'C:\Users\sheeb\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\sheeb\AppData\Local\Programs\Python\Python311\python.exe'
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Spark Second") \
    .master("local[*]") \
    .getOrCreate()

dataa = [
    (1, "Elsa", 25, 1000),
    (2, "John", 30, 2000),
    (3, "Ana", 28, 1500),
    (4, "Mike", 35, 3000),
    (5, "Sara", 38, 3800),
    (6, "Doe", 40, 4000),
    (7, "James", 27, 3000),
    (8, "Rick", 30, 2500),
    (9, "Shane", 30, 2000),
    (10, "Carl", 25, 2200)
]

dffs = spark.createDataFrame(dataa, ["id", "Name", "Age", "Bonus"])
#dffs.show()

dffs.createOrReplaceTempView("Workers")
spark.sql("select * from Workers").show()
spark.sql("select name, bonus from Workers").show()
spark.sql("select * from Workers where age > 30").show()
spark.sql("select name from Workers where name like 'J%'").show()
spark.sql("select * from Workers order By bonus desc").show()

empData = [
    [101, "John", "Doe", "Sales", 55000, "2018-04-23", 201],
    [102, "Jane", "Smith", "Finance", 62000, "2019-06-12", 202],
    [103, "Michael", "Brown", "IT", 72000, "2017-09-30", 203],
    [104, "Linda", "Johnson", "HR", 50000, "2020-01-15",201],
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

df = spark.createDataFrame(data = empData, schema = empSchema)
#df.show()

#General df operations
#df.show()
rows = df.collect()
#print(rows)
takes = df.take(2)
#print(takes)
#df.printSchema()
num = df.count()
#print(num)
#df.select("firstName", "lastName", "department").show()
#df.select(df.empid, (df.salary+100).alias("Salary")).show()
#df.filter(df.department == "Finance").show()
#df.where(df.department == "IT").show()
#df.filter(df.firstName.like ("%n")).show()
#df.sort(df.salary.desc()).show()
#df.orderBy(df.empid.desc()).show()
#df.groupBy("department").avg("salary").show()
#df.groupBy("department").agg({"empid" : "count", "salary" : "sum"}).show()
#df.describe().show()
#df.describe("salary", "empid").show()
print(df.columns)


#String operations
data = [(" Alice ",), ("bob",), ("Charlie123",), ("eve_smith",)]
dff = spark.createDataFrame(data, ["name"])
dff.show(truncate=False)

#upper
dff.select("name", upper("name").alias("UpperName")).show()
#trim
dff.select("name", trim("name").alias("TrimmedName")).show()
#l
dff.select("name", ltrim("name")).show()
#rtrim
dff.select("name", rtrim("name")).show()
#substring_index
dff.select("name", substring_index("name", "_", 1)).show()
#substring
dff.select("name", substring("name", 1, 5).alias("Substring")).show()
#split
dff.select("name", split("name", "_").alias("SplittedString")).show()
#repeat
dff.select("name", repeat("name", 2).alias("Repeated")).show()
#rpad
dff.select("name", rpad("name", 12, "*").alias("R Padded")).show()
#lpad
dff.select("name", lpad("name", 10, "*").alias("L Padded")).show()
#regexp_replace
from pyspark.sql.functions import regexp_replace
dff.select("name", regexp_replace("name", "[0-9]", "").alias("Replaced")).show()
dff.select("name", regexp_replace("name", "[0-9]", "_").alias("Replaced")).show()
dff.select("name", regexp_replace("name", "i", "!").alias("Replaced")).show()
#regexp_extract
from pyspark.sql.functions import regexp_extract
dff.select("name", regexp_extract("name", "([A-Aa-z]+)", 1).alias("Extracted")).show()
dff.select("name", regexp_extract("name", "([0-9]+)", 1).alias("Extracted")).show()
#lower
dff.select("name", lower("name").alias("Lowercase")).show()
#length
dff.select("name", length("name").alias("Length")).show()
#instr
dff.select("name", instr("name", "li")).show()
#initcap
dff.select("name", initcap("name")).show()

#Numeric operations
datas = [
    (1, 10.5),
    (2, -20.3),
    (3, 30.7),
    (4, -40.1)
]
dffss = spark.createDataFrame(datas, ["id", "value"])
dffss.show()

#Sum
dffss.select(sum("value")).show()
#avg
dffss.select(avg("value")).show()
#min
dffss.select(min("value")).show()
#max
dffss.select(max("value")).show()
#abs
dffss.select(abs("value")).show()
#round
dffss.select(round("value", 0), round("value", 1), round("value", 2)).show()
