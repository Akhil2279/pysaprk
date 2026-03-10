from Tools.demo.sortvisu import distinct
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("transformations").getOrCreate()


#transformation

#map transformation

# my_list = ["akhil", "stalin", "abdul" , "ravi"]
#
# rdd = spark.sparkContext.parallelize(my_list)
#
# print(rdd.collect())
#
# new_rdd = rdd.map(lambda x : x.upper())
#
# print(new_rdd.collect())

#
# my_list = [1,2,3,4,5]
#
# rdd = spark.sparkContext.parallelize(my_list)
#
# print(rdd.collect())
#
# new_rdd = rdd.map(lambda x : x * 5)
#
# print(new_rdd.collect())


# FILTER transformation

# rdd = spark.sparkContext.parallelize([1,2,3,4,5,6])
# print(rdd.collect())
# filter_rdd = rdd.filter(lambda x : x > 2)
# print(filter_rdd.collect())

# FLAT MAP
#flattern+map = [[1,2,3] [4,5,6]] = [1,2,3,4,5,6]

# rdd = spark.sparkContext.parallelize(["hello, akhil" , "how are you" , "what" , "are you doing"])
# print(rdd.collect())
# fm_rdd = rdd.flatMap(lambda x : x.split(" "))
# print(fm_rdd.collect())

#Groupby key

# my_list = [(25,60000),(30,60000), (35, 80000), (40, 190000) ]
# rdd = spark.sparkContext.parallelize(my_list)
# print(rdd.collect())
# grouped_rdd = rdd.groupByKey()
# print(grouped_rdd.collect())
# grouped_rdd1 = grouped_rdd.mapValues(lambda x : sum(x))
# print(grouped_rdd1.collect())


#reduceByKey()
# rdd = spark.sparkContext.parallelize([(25,60000),(30,60000), (35, 80000), (30, 190000) ])
# print(rdd.collect())
# grouped_rdd = rdd.reduceByKey(lambda x,y : x+y)
# print(grouped_rdd.collect())

# Joins  it will combain the given values

# rdd1 = spark.sparkContext.parallelize([(1,("Akhil", 25)),(2, ("Stalin" , 30)),(3, ("Charlie" , 35)) ])
# rdd2 = spark.sparkContext.parallelize([(1,("New York", "Engineer" )),(2, ("San Francisco", "Doctor" )),(3, ("India" , "SOftWare")) ])
# joined_rdd = rdd1.join(rdd2)
# print(joined_rdd.collect())

# Distinct  it will remove the duplicate values

# rdd = spark.sparkContext.parallelize([1,2,3,4,1,5,2,6,4])
# distinct_rdd = rdd.distinct()
# print(distinct_rdd.collect())


# sortBy  it will order by values from ascending order

# rdd = spark.sparkContext.parallelize([3,4,7,1,2,9,6,8,5,0])
# rdd_sort = rdd.sortBy(lambda x : x, ascending=True)
# print(rdd_sort.collect())

#Union Concatenates two rdds and return new rdd.

# rdd1 = spark.sparkContext.parallelize([1,2,3])
# rdd2 = spark.sparkContext.parallelize([4,5,6])
# union_rdd = rdd1.union(rdd2)
# print(union_rdd.collect())

# Cartesian it will return the cartesian products as a nuw rdd

# rdd1 = spark.sparkContext.parallelize([1,2,3])
# rdd2 = spark.sparkContext.parallelize(["a", "b", "c"])
# cartesian_rdd = rdd1.cartesian(rdd2)
# print(cartesian_rdd.collect())

# output [(1, 'a'), (1, 'b'), (1, 'c'), (2, 'a'), (2, 'b'), (2, 'c'), (3, 'a'), (3, 'b'), (3, 'c')]

# repartition: repartitions the RDD into a specified number of partitions. repartition can increase or decrease and it will create nuw partitions to shuffle the data.

# data = [("A",10),("B",20),("C",30),("D",40)]
# df = spark.createDataFrame(data, ["name","value"])
# print(df.rdd.getNumPartitions())
# df2 = df.repartition(4)
# print(df2.rdd.getNumPartitions())

#output
# 16
# 4

# Coalesce : Only decrease the number of partitions.It is mainly used when you want to combine existing partitions into fewer partitions to improve performance or reduce output files.

data = [("A",10),("B",20),("C",30),("D",40)]
df = spark.createDataFrame(data, ["name","value"])
print("Before:", df.rdd.getNumPartitions())
df2 = df.coalesce(4)
print("After:", df2.rdd.getNumPartitions())

#output
# 16
# 4


