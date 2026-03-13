from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Write Parquet").getOrCreate()

data = [
("Akhil",25,"Bangalore",20000),
("Rahul",28,"Hyderabad",50000),
("Neha",24,"Delhi",25000),
("Ali",22,"Andhra",35000)
]

columns = ["name","age","city","salary"]

df = spark.createDataFrame(data, columns)

# modify salary
new_df = df.withColumn("salary", col("salary") + 5000)

#

# write updated dataframe
new_df.write.mode("overwrite") \
.parquet("C:/Users/akhil/OneDrive/Documents/pyspark/parquet_data")

#Overwrite Partition
df.write.mode("overwrite") \
.partitionBy("city") \
.parquet("C:/Users/akhil/OneDrive/Documents/pyspark/parquet_data")

#This is used when writing partitioned data.
#Only the specific partition is replaced, not the whole dataset

new_df.show()
new_df.printSchema()










# Spark stores data in multiple partition files because Spark is a distributed system
# why we have to use overwrite
# Delete the existing folder
# Create new Parquet files
# Store the new data

