from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("First program").getOrCreate()
#
# #list/array to rdd - parallelize()
#
# my_list = ["Hello World!!"]
# print (type(my_list))
#
# my_rdd = spark.sparkContext.parallelize(my_list)
# print (my_rdd.collect())   #print(my_rdd) → shows only the RDD object reference, not the data.
#                            #print(my_rdd.collect()) → executes the RDD and prints the actual data as a lis
# print (type(my_rdd))

#HDFS/local machines to RDD  --> textFile()
# my_rdd = spark.sparkContext.textFile("C://Users//akhil//OneDrive//Desktop//pyspark//example1.txt")
# print(type(my_rdd))
# print(my_rdd.collect())


#rdd to new rdd
rdd = spark.sparkContext.parallelize([1,2,3,4,5])
new_rdd = rdd.filter(lambda x: x>=3)
print(rdd.collect())
print(new_rdd.collect())


# #sum of Numbers
# spark = SparkSession.builder.appName("RDDSum").getOrCreate()
# sc = spark.sparkContext
#
# numbers = [10, 20, 30, 40]
#
# rdd = sc.parallelize(numbers)
#
# total_sum = rdd.reduce(lambda x, y: x + y)
#
# print("Total Sum:", total_sum)
#
#
#
# #even numbers
#
# spark = SparkSession.builder.appName("RDDEvenNumbers").getOrCreate()
# sc = spark.sparkContext
#
# numbers = [1,2,3,4,5,6,7,8]
#
# rdd = sc.parallelize(numbers)
#
# even_numbers = rdd.filter(lambda x: x % 2 == 0)
#
# print(even_numbers.collect())


