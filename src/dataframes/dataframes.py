
#Filter High Salary

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("FilterSalary").getOrCreate()

# df = spark.read.csv("datasets/employees.csv", header=True, inferSchema=True)
#
# result = df.filter(df.salary > 60000)
#
# result.show()
#
#
# #Average Salary
#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import avg
#
# spark = SparkSession.builder.appName("AverageSalary").getOrCreate()
#
# df = spark.read.csv("datasets/employees.csv", header=True, inferSchema=True)
#
# result = df.agg(avg("salary").alias("average_salary"))
#
# result.show()
#
# #Salary by Department
#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import sum
#
# spark = SparkSession.builder.appName("DepartmentSalary").getOrCreate()
#
# df = spark.read.csv("datasets/employees.csv", header=True, inferSchema=True)
#
# result = df.groupBy("department").agg(sum("salary").alias("total_salary"))

#result.show()



# Create Data Frame

schema = StructType([
    StructField ( "emp_id" , IntegerType() , True),
    StructField("name" , StringType() , True),
    StructField("dep" , StringType() , True),
    StructField("Sal" , IntegerType() , True)
])

data = [
    (1 , "Akhil" , "IT" , 60000),
    (2 , "Anil" , "Sales" , 50000),
    (3 , "Stalin" , "Medical" , 70000),
    (4, "Ravi" , "Medical" , 40000),
    (5, "Bhanu" , "Hr" , 55000)
]

df = spark.createDataFrame(data , schema)
#
# df.show()


# Write data frame to csv

#df.write.format("CSV").option("header" , True).mode("overwrite").save("location")

# Write data frame with parquet

#df.write.format("parquet").mode("overwrite").save("location")

#write dataframe to json with schema

df.write.format("json").mode("overwrite").save("location")