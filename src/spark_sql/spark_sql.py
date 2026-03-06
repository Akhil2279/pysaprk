
#High Salary

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

df = spark.read.csv("datasets/employees.csv", header=True, inferSchema=True)

df.createOrReplaceTempView("employees")

result = spark.sql("""
SELECT *
FROM employees
WHERE salary > 60000
""")

result.show()


#Average Salary by Department


spark = SparkSession.builder.appName("SparkSQLAvg").getOrCreate()

df = spark.read.csv("datasets/employees.csv", header=True, inferSchema=True)

df.createOrReplaceTempView("employees")

result = spark.sql("""
SELECT department, AVG(salary) AS avg_salary
FROM employees
GROUP BY department
""")

result.show()

#Cache DataFrame


spark = SparkSession.builder.appName("CacheDataFrame").getOrCreate()

df = spark.read.csv("datasets/employees.csv", header=True, inferSchema=True)

df.cache()

df.show()

print("Is Cached:", spark.catalog.isCached("employees"))