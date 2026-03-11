from pyspark.sql.functions import explode , explode_outer , posexplode_outer
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("explode_example").getOrCreate()

data = [
("Akhil", ["Python","SQL","Spark"]),
("Rahul", None),
("Anil", ["SQL","Excel"])
]

df = spark.createDataFrame(data, ["name","skills"])

df.show(truncate=False)

# Apply explode()
#df.select("name", explode("skills").alias("skill")).show()

# explode_outer()
#df.select("name", explode_outer("skills").alias("skill")).show()

# posexplode_outer()
df.select("name", posexplode_outer("skills")).show()