import os
import sys
os.environ['PYSPARK_PYTHON'] = r'C:\Users\sheeb\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\sheeb\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import countDistinct

spark = SparkSession.builder\
        .appName("Spark func")\
        .master("local[*]")\
        .getOrCreate()

#Datetime functions
dates = [("2025-09-25",), ("2025-10-01",)]
df = spark.createDataFrame(dates, ["order_date"])
df.show()

#current_date
df.select("order_date", current_date().alias("Current date")).show()
#current_timestamp
df.select("order_date", current_timestamp().alias("Current date and time")).show(truncate=False)
#date_add
df.select("order_date", date_add("order_date", 5).alias("Date after 5 days")).show()
#date_diff
df.select("order_date", date_diff("order_date", lit('2025-10-08').cast("date")).alias("Date difference")).show()
#year
df.select("order_date", year("order_date").alias("Year")).show()
#month
df.select("order_date", month("order_date").alias("Month")).show()
#day
df.select("order_date", day("order_date").alias("Day of the date(DD)")).show()
#to_date
df.select("order_date", to_date("order_date").alias("Proper date")).show()
#date_format
df.select("order_date", date_format("order_date", 'dd-MM-yyyy').alias("Formatted date")).show()
#date_format
df.select("order_date", date_format("order_date", "yyyy/MM/dd").alias("Formatted date")).show()
#date_format (Day of the week)
df.select("order_date", date_format("order_date", 'EEEE').alias("Day of the week")).show()
#date_format (Abbreviated day of the week) - E/EEE
df.select("order_date", date_format("order_date", 'E').alias("Abbreviated day")).show()

#Aggregation functions
data = [
    ("A", 10),
    ("A", 20),
    ("A", 20),
    ("B", 30),
    ("B", 40),
    ("C", 50)
]
dff = spark.createDataFrame(data, ["category", "value"])
dff.show()
#mean/avg
dff.groupBy("category").agg(mean("value")).show()
dff.agg(avg("value")).show()
#collect_list
dff.groupBy("category").agg(collect_list("value")).show()
dff.agg(collect_list("value")).show(truncate = False)
#collect_set
dff.groupBy("category").agg(collect_set("value")).show()
dff.agg(collect_set("value")).show(truncate = False)
#countDistinct
dff.groupBy("category").agg(countDistinct("value")).show()
dff.agg(countDistinct("value")).show(truncate = False)
#count
dff.groupBy("category").agg(count("value")).show()
dff.agg(count("value")).show(truncate = False)
#first
dff.groupBy("category").agg(first("value")).show()
dff.agg(first("value")).show(truncate = False)
#last
dff.groupBy("category").agg(last("value")).show()
dff.agg(last("value")).show(truncate = False)
#max
dff.groupBy("category").agg(max("value")).show()
dff.agg(max("value")).show(truncate = False)
#min
dff.groupBy("category").agg(min("value")).show()
dff.agg(min("value")).show(truncate = False)
#sum
dff.groupBy("category").agg(sum("value")).show()
dff.agg(sum("value")).show(truncate = False)

#Mathematical functions
datas = [
    (-5.5,),
    (3.2,),
    (9.0,),
    (0.0,)
]
dffs = spark.createDataFrame(datas, ["number"])
dffs.show()
#abs
dffs.select("number", abs("number")).show()
#ceil
dffs.select("number", ceil("number")).show()
#floor
dffs.select("number", floor("number")).show()
#exp
dffs.select("number", exp("number")).show()
#log
dffs.select("number", log("number")).show()
#power
dffs.select("number", power("number", 2)).show()
#sqrt
dffs.select("number", sqrt("number")).show()
