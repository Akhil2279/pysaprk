from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("csv_to_parquet") \
    .config("spark.hadoop.fs.file.impl","org.apache.hadoop.fs.RawLocalFileSystem") \
    .config("spark.hadoop.io.native.lib.available","false") \
    .getOrCreate()

df = spark.read.csv(
    "C:/Users/akhil/OneDrive/Documents/employees.csv",
    header=True,
    inferSchema=True
)

df.write.mode("overwrite").parquet(
"C:/Users/akhil/OneDrive/Documents/employees_parquet"
)

df.show()