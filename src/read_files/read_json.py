# Read Json

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadJSON").getOrCreate()

df = spark.read.json("C:/Users/akhil/OneDrive/Documents/data.json")

df.show()
df.printSchema()