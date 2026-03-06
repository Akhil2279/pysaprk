
#Annual Salary Column

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("AnnualSalary").getOrCreate()

df = spark.read.csv("datasets/employees.csv", header=True, inferSchema=True)

df = df.withColumn("annual_salary", col("salary") * 12)

df.show()

#Uppercase Names

from pyspark.sql import SparkSession
from pyspark.sql.functions import upper

spark = SparkSession.builder.appName("UppercaseNames").getOrCreate()

df = spark.read.csv("datasets/employees.csv", header=True, inferSchema=True)

df = df.withColumn("name_upper", upper(df.name))

df.show()

#Read Text File

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadTextFile").getOrCreate()

sc = spark.sparkContext

text_rdd = sc.textFile("datasets/sample.txt")

print("Total Lines:", text_rdd.count())