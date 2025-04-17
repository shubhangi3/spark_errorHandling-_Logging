from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# Initialize Spark session

spark = SparkSession.builder \
    .appName("simpletrasformation") \
    .getOrCreate()

# Sample data
data =[
    ("Geet",25),
    ("Swara",18),
    ("Aarushi",22),
    ("Adi",10),
    ("Toshu",11)
    ]
# Define schema
columns = ["Name","Age"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

print("Original Data:")
df.show()

# Transformation 1: Filter age > 20
df_filtered = df.filter(col("Age") > 20)

# Transformation 2: Add a new column
df_final = df_filtered.withColumn("AgeAfter5Years", col("Age") + 5)

# Show result
print("Transformed Data:")
df_final.show()

# Stop Spark
spark.stop()