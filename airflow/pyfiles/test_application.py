from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Spark Connection Test").getOrCreate()

# Sample data for the DataFrame
data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]

# Create a DataFrame with columns 'Name' and 'Id'
df = spark.createDataFrame(data, ["Name", "Id"])

# Display the contents of the DataFrame
df.show()

# Stop the Spark session to release resources
spark.stop()
