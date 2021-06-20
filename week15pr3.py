from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType

appName = "JSON file to Spark Data Frame"
master = "local"

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

# Create a schema for the dataframe
schema = StructType([
    StructField('color', StringType(), True),
    StructField('value', StringType(), True)
])

# Create data frame
json_file_path = 'example2.json'
df = spark.read.json(json_file_path, schema, multiLine=True)
print(df.schema)
df.show()