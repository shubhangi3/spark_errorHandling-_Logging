from pyspark.sql import SparkSession
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
try:
    spark = SparkSession.builder \
        .appName("MySparkApp") \
        .master("local[*]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    logger.info("SparkSession initialized successfully.")
except Exception as e:
    logger.error(f"Error initializing SparkSession: {e}")
    raise
