from Tools.scripts.generate_opcode_h import header
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("Read CSV").getOrCreate()

emp_df = spark.read.option("header", True).option("inferSchema" , True).csv("C:/Users/akhil/OneDrive/Documents/employees.csv")

emp_df.show()

emp_df.printSchema()










# input_schema = StructType([
#     StructField("emp_id",IntegerType(),True),
#     StructField("emp_name",StructType(), True),
#     StructField("salary", IntegerType(), True),
#     StructField("Country", StringType(), True)
# ])
#
# #df.write.option("header", True).csv("C:/Users/akhil/OneDrive/Desktop/pyspark/src/dataframes/output.csv")
#
# df = spark.read\
#     .option("header",True)\
#     .option("delimiter", "|")\
#     .option("inferSchema", True)\
#     .schema(input_schema)\
#     .csv("C:/Users/akhil/OneDrive/Desktop/pyspark/src/dataframes/output.csv")

# Features:
# 1. Header ("header": True)
# 2. FIle delimiter ( default delimiter is comma)
# 3. inferSchema : It is used to provide the schema to database
#Define Schema