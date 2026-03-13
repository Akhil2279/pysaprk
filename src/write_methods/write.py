from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read CSV").getOrCreate()

emp_df = spark.read.option("header", True).option("inferSchema" , True).csv("C:/Users/akhil/OneDrive/Documents/pyspark/employees.csv")

sorted_df = emp_df.orderBy("salary")

sorted_df.write.mode("overwrite") \
    .option("header", True) \
    .csv("C:/Users/akhil/OneDrive/Documents/sorted_employees")
emp_df.show()
sorted_df.show()