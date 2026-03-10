
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("Aggregate Functions").getOrCreate()

emp_df = spark.read.option("header", True).csv("C:/Users/akhil\OneDrive\Documents\employees.csv")

emp_df.show()

#Total Salary
emp_df.select(sum("salary")).show()

#Average Salary
emp_df.select(avg("salary")).show()

#Maximum Salary
emp_df.select(max("salary")).show()

#Minimum Salary
emp_df.select(min("salary")).show()

#Count Employees
emp_df.select(count("emp_id")).show()

#Distinct Departments
emp_df.select(countDistinct("dept_id")).show()

#Collect Employee Names by Department
emp_df.groupBy("dept_id") \
    .agg(collect_list("emp_name")) \
    .show()

#Collect Unique Employees
emp_df.groupBy("dept_id") \
    .agg(collect_set("emp_name")) \
    .show()