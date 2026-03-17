
from pyspark.sql import SparkSession
import time

Spark = SparkSession.builder.appName("Joins").getOrCreate()
print("Spark UI running at:", Spark.sparkContext.uiWebUrl)

emp_df = Spark.read.option("header", True).csv("C:/Users/akhil/OneDrive/Documents/pyspark/employees.csv")

emp_df.show()


dept_df = Spark.read.option("header", True).csv("C:/Users/akhil/OneDrive/Documents/pyspark/department.csv")

#dept_df.show()

emp_df.createOrReplaceTempView("employees")
dept_df.createOrReplaceTempView("departments")

# Inner Join : used to retrive the matching records

Spark.sql("""
SELECT e.emp_name, d.dept_name
FROM employees e
INNER JOIN departments d
ON e.dept_id = d.dept_id
""").show()


# Left join : Show all employees, even if they don't have a department.
Spark.sql("""
SELECT e.emp_name, d.dept_name
FROM employees e
LEFT JOIN departments d
ON e.dept_id = d.dept_id
""").show()

# Right join : Show all departments, even if no employees work there.
Spark.sql("""
SELECT e.emp_name, d.dept_name
FROM employees e
RIGHT JOIN departments d
ON e.dept_id = d.dept_id
""").show()

# Full join : Return all employees and all departments.
Spark.sql("""
SELECT e.emp_name, d.dept_name
FROM employees e
FULL JOIN departments d
ON e.dept_id = d.dept_id
""").show()

# cross join : Every employee joins with every department.

Spark.sql("""
SELECT e.emp_name, d.dept_name
FROM employees e
CROSS JOIN departments d
""").show()

time.sleep(120)