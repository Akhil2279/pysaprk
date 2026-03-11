from pyspark.sql import SparkSession
from pyspark.sql.functions import size, array_position, array_remove, array_contains

spark = SparkSession.builder.appName("array_functions").getOrCreate()

data = [
("Akhil", ["Python", "SQL", "Spark"]),
("Rahul", ["Java", "Python"]),
("Anil", ["SQL", "Excel"]),
("Priya", ["Python", "Spark"])
]

df = spark.createDataFrame(data, ["name", "skills"])

df.show(truncate=False)


# this shows who know python
df.select("name", array_contains("skills", "Python").alias("knows_python")).show()


# ARRAY_LENGTH()

df.select("name", size("skills").alias("skill_count")).show()

# ARRAY_POSITION()
df.select("name", array_position("skills", "Python").alias("position")).show()

# ARRAY_REMOVE()
df.select("name", array_remove("skills", "Python").alias("updated_skills")).show(truncate=False)

# Find employees who know Spark.
df.filter(array_contains("skills", "Spark")).show()

# Count number of skills for each employee.
df.select("name", size("skills").alias("skill_count")).show()

# Remove Python skill from all employees.
df.select("name", array_remove("skills", "Python")).show()








