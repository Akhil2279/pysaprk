from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Action Programs").getOrCreate()

# Collect () : returns all elements of the rdd as an array to driver program.

rdd = spark.sparkContext.parallelize([1,2,3,4,5,6,7,8,9])
print(rdd.collect())  # → [1, 2, 3, 4, 5, 6, 7, 8, 9]
print(rdd.count())  # → 9
print(rdd.take(4))   # → [1, 2, 3, 4]
print(rdd.first())  # → 1
print(rdd.foreach(lambda x : print(x))) # → 1 2 3 4 5 6 7 8 9
print(rdd.reduce(lambda x,y : x+y)) # → 45
rdd.saveAsTextFile("c://output.txt")  # → saves the RDD elements as text files in the specified folder
rdd.saveAsSequenceFile("c://output")  # → saves the RDD as a Hadoop sequence file in the specified folder
