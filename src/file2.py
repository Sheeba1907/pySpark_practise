import os
import sys
from pyspark.sql.types import *

os.environ['PYSPARK_PYTHON'] = r'C:\Users\sheeb\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\sheeb\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Spark Second") \
    .master("local[*]") \
    .getOrCreate()

df = spark.createDataFrame([
    (1, "John", 80),
    (2, "James", 90),
    (3, "Doe", 75),
    (4, "Davis", 85)
], ["id", "name", "score"])

#df.show()

#df.select("Name", "score").show()
#df.filter(df.score>85).show()
#df.withColumn("max score", col("score")+5).show()
#df.groupBy("id").avg("score").show()

#print(df.collect())
#print("Row count:", df.count())
#print("First row:", df.first())
#print("Taking 2 rows:", df.take(2))

#RDD creation
rdd = spark.sparkContext.parallelize([("Alice", 10, 123), ("Bob", 30, 456),("Chad", 50, 789),
                                      ("Doe", 70, 147), ("Ellie", 90, 258)])
#print(rdd.collect())
rdd1 = rdd.map(lambda x: (x[0], x[1]*2))
#print(rdd1.collect())
rdd2 = rdd.flatMap(lambda x: (x[0], x[1]*2))
#print(rdd2.collect())

# union()
rdd11 = spark.sparkContext.parallelize([1, 2])
rdd21 = spark.sparkContext.parallelize([3, 4])
#print("union():", rdd11.union(rdd21).collect())

# intersection()
#print("intersection():", rdd11.intersection(rdd21).collect())

# groupByKey() & reduceByKey()
pairs = spark.sparkContext.parallelize([("a", 1), ("b", 2), ("a", 3)])
#print("groupByKey():", [(k, list(v)) for k, v in pairs.groupByKey().collect()])

#print("reduceByKey():", pairs.reduceByKey(lambda a, b: a + b).collect())

#print("\n========== RDD ACTIONS ==========\n")

# collect()
#print("collect():", rdd.collect())

# count()
#print("count():", rdd.count())

# first()
#print("first():", rdd.first())

# take()
#print("take(3):", rdd.take(3))

# reduce()
#print("reduce(sum):", rdd.reduce(lambda a, b: a + b))

#Reading file
red = spark.sparkContext.textFile(r"C:\Users\sheeb\Downloads\archive\Sales.csv")
#print(red.take(10))

#RDD to DF conversion
rdd = spark.sparkContext.parallelize([("Alice", 10, 123), ("Bob", 30, 456),("Chad", 50, 789),
                                      ("Doe", 70, 147), ("Ellie", 90, 258)])
dff = rdd.toDF(["Name", "Id", "Number"])
#dff.show()

schemas = StructType([StructField(name="Name", dataType=StringType()),
                        StructField(name="Id", dataType=IntegerType()),
                        StructField(name="Number", dataType=IntegerType())])

dfs = spark.createDataFrame(data=rdd, schema=schemas)
#dfs.show()

#PySpark SQL
dfdata = spark.read.csv(r"C:\Users\sheeb\Downloads\sample_data.csv")
dfdata.show()



