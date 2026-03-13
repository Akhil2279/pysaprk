from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ParquetExample").getOrCreate()


# parquet Read
data = [
("Akhil",25,"Bangalore"),
("Rahul",28,"Hyderabad"),
("Neha",24,"Delhi")
]

columns = ["name","age","city"]

df = spark.createDataFrame(data, columns)

# write parquet file
df.write.mode("overwrite").parquet("C:/Users/akhil/OneDrive/Documents/parquet_data")
df.show()

# Spark stores data in multiple partition files because Spark is a distributed system
# why we have to use overwrite
# Delete the existing folder
# Create new Parquet files
# Store the new data


# spark = SparkSession.builder \
#     .appName("csv_to_parquet") \
#     .config("spark.hadoop.fs.file.impl","org.apache.hadoop.fs.RawLocalFileSystem") \
#     .config("spark.hadoop.io.native.lib.available","false") \
#     .getOrCreate()
#
# df = spark.read.csv(
#     "C:/Users/akhil/OneDrive/Documents/employees.csv",
#     header=True,
#     inferSchema=True
# )
#
# df.write.mode("overwrite").parquet(
# "C:/Users/akhil/OneDrive/Documents/employees_parquet"
# )
#
# df.show()