
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("First program").getOrCreate()

#list/array to rdd - parallelize()

my_list = ["Hello World!!"]
print (type(my_list))

my_rdd = spark.sparkContext.parallelize(my_list)
print (my_rdd.collect())   #print(my_rdd) → shows only the RDD object reference, not the data.
                           #print(my_rdd.collect()) → executes the RDD and prints the actual data as a lis
print (type(my_rdd))