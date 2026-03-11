from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("UDF_Practice").getOrCreate()

data = [
("Akhil", 25, 60000),
("Rahul", 30, 40000),
("Neha", 28, 70000),
("Kiran", 35, 30000),
("Priya", 26, 50000)
]

df = spark.createDataFrame(data, ["name", "age", "salary"])

df.show()

# Create Python Function
def salary_category(salary):
    if salary >= 60000:
        return "High"
    elif salary >= 40000:
        return "Medium"
    else:
        return "Low"

# Convert to UDF
salary_udf = udf(salary_category, StringType())
# Apply UDF
df.withColumn("salary_category", salary_udf(df["salary"])).show()