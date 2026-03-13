from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ParquetExample").getOrCreate()


# parquet Read
data = [
("Akhil",25,"Bangalore",20000),
("Rahul",28,"Hyderabad",50000),
("Neha",24,"Delhi",25000),
("Ali", 22,"Andhra", 35000 )
]

columns = ["name","age","city","salary"]

df = spark.createDataFrame(data, columns)

# write parquet file
df.write.mode("overwrite").parquet("C:/Users/akhil/OneDrive/Documents/parquet_data")
df.show()

# Spark stores data in multiple partition files because Spark is a distributed system
# why we have to use overwrite
# Delete the existing folder
# Create new Parquet files
# Store the new data