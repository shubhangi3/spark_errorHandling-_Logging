from pyspark.sql import SparkSession

# Step 1: Start Spark Session
spark = SparkSession.builder \
    .appName("Narrow Transformation Example - Map and Filter") \
    .getOrCreate()

# Step 2: Create RDD
data = [
    ("Shubhangi", 60000),
    ("Raj", 45000),
    ("Anita", 75000),
    ("Vikram", 40000),
    ("Priya", 51000),
    ("Swara",85000),
    ("Ravi",65000)
]
rdd = spark.sparkContext.parallelize(data)

# Step 3: Narrow Transformation - Filter and Map
# Filter employees earning 50,000 or more
filtered_rdd = rdd.filter(lambda x: x[1] >= 50000)

# Map to a readable message
message_rdd = filtered_rdd.map(lambda x: f"Employee: {x[0]}, Salary: {x[1]}")

# Step 4: Collect and Print
for message in message_rdd.collect():
    print(message)

# Step 5: Stop Spark session
spark.stop()
