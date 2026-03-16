from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FullLoad").getOrCreate()

df = spark.read.csv("C:/Users/akhil/OneDrive/Documents/pyspark/employees_day1.csv", header=True, inferSchema=True)

df.write.mode("overwrite").parquet("warehouse/employees")

