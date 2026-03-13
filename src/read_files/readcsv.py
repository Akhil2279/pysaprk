
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read CSV").getOrCreate()

emp_df = spark.read.option("header", True).option("inferSchema" , True).csv("C:/Users/akhil/OneDrive/Documents/pyspark/employees.csv")

emp_df.show()

emp_df.printSchema()


#In PySpark, the write method works only with a DataFrame. So if your data is in Excel,
# you cannot directly write it to CSV using Spark unless it is first converted into a DataFrame.


# Features:
# 1. Header ("header": True)
# 2. FIle delimiter ( default delimiter is comma)
# 3. inferSchema : It is used to provide the schema to database
#Define Schema




