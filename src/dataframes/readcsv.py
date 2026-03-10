from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("Read CSV").getOrCreate()

data = [
    ("Akhil", 25, "India"),
    ("Ravi", 30, "USA"),
    ("John", 28, "UK")
]

columns = ["name", "age", "country"]

df = spark.createDataFrame(data, columns)

df.show()

input_schema = StructType([
    StructField("emp_id",IntegerType(),True),
    StructField("emp_name",StructType(), True),
    StructField("salary", IntegerType(), True),
    StructField("Country", StringType(), True)
])

#df.write.option("header", True).csv("C:/Users/akhil/OneDrive/Desktop/pyspark/src/dataframes/output.csv")

df = spark.read\
    .option("header",True)\
    .option("delimiter", "|")\
    .option("inferSchema", True)\
    .schema(input_schema)\
    .csv("C:/Users/akhil/OneDrive/Desktop/pyspark/src/dataframes/output.csv")

# Features:
# 1. Header ("header": True)
# 2. FIle delimiter ( default delimiter is comma)
# 3. inferSchema : It is used to provide the schema to database
#Define Schema