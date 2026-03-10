from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Mathematical Functions").getOrCreate()

emp_df = spark.read.option("header", True).csv("C:/Users/akhil\OneDrive\Documents\employees.csv")

emp_df.show()

#Absolute Value
emp_df.select("emp_name", abs(col("salary") - 60000)).show()

#Ceiling
emp_df.select("emp_name", ceil(col("salary")/1000)).show()

#Floor
emp_df.select("emp_name", floor(col("salary")/1000)).show()

#Square Root
emp_df.select("emp_name", sqrt(col("salary"))).show()

#Power
emp_df.select("emp_name", power(col("age"),2)).show()

#Log
emp_df.select("emp_name", log(col("salary"))).show()