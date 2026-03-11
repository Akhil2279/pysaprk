from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, col , lag , lead

Spark = SparkSession.builder.appName("Joins").getOrCreate()

df = Spark.read.option("header", True).csv("C:/Users/akhil\OneDrive\Documents\employees.csv")

emp_df.show()


df = Spark.read.option("header", True).csv("C:/Users/akhil\OneDrive\Documents\department.csv")

#dept_df.show()

df.createOrReplaceTempView("employees")
df.createOrReplaceTempView("departments")


# ROW_NUMBER
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

df.withColumn("row_number",row_number().over(window_spec)).show()


# RANK
df.withColumn("rank",rank().over(window_spec)).show()

# DENSE_RANK
df.withColumn("dense_rank",dense_rank().over(window_spec)).show()

# LEAD (next salary)

df.withColumn("next_salary",lead("salary").over(window_spec)).show()

# LAG (previous salary)
df.withColumn("previous_salary",lag("salary").over(window_spec)).show()

# Top 2 salaries per department
df.withColumn("rank",dense_rank().over(window_spec)).filter(col("rank")<=2).show()

