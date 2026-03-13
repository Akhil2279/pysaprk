from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ORCExample").getOrCreate()

data = [
("Akhil",25,"Bangalore"),
("Rahul",28,"Hyderabad"),
("Neha",24,"Delhi")
]

columns = ["name","age","city"]

df = spark.createDataFrame(data, columns)

# write ORC file
df.write.mode("overwrite").orc("C:/Users/akhil/OneDrive/Documents/pyspark/orc_data")

df.show()