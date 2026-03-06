
#Filter High Salary

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FilterSalary").getOrCreate()

df = spark.read.csv("datasets/employees.csv", header=True, inferSchema=True)

result = df.filter(df.salary > 60000)

result.show()


#Average Salary

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder.appName("AverageSalary").getOrCreate()

df = spark.read.csv("datasets/employees.csv", header=True, inferSchema=True)

result = df.agg(avg("salary").alias("average_salary"))

result.show()

#Salary by Department

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("DepartmentSalary").getOrCreate()

df = spark.read.csv("datasets/employees.csv", header=True, inferSchema=True)

result = df.groupBy("department").agg(sum("salary").alias("total_salary"))

result.show()