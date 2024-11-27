from pyspark.sql import SparkSession

# Initialize SparkSession with Hive support
spark = SparkSession.builder.appName("HiveExample").enableHiveSupport().getOrCreate()

# Define a Hive table name
hive_table = "my_database.my_table"

# Read from a Hive table
df = spark.read.table(hive_table)

# Perform some transformations (example)
df_filtered = df.filter(df["age"] > 30)

# Show the data
df_filtered.show()

# Write the result back to a Hive table
df_filtered.write.mode("overwrite").saveAsTable("my_database.my_filtered_table")

# Alternatively, you can write to a file (CSV, Parquet, etc.)
df_filtered.write.mode("overwrite").parquet("s3a://path/to/output/directory")
