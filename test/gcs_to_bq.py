from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# Initialize Spark session with BigQuery connector and configurations
spark = (
    SparkSession.builder.appName("GCS to BigQuery")
    .config(
        "spark.jars",
        "gs://spark-lib/bigquery/ \
            spark-bigquery-with-dependencies_2.12-0.41.0.jar",
    )
    .config(
        "spark.sql.catalog.bigquery",
        "com.google.cloud.spark.bigquery.v2.BigQueryCatalog",
    )
    .config("spark.sql.catalog.bigquery.auth.service.account.enable", "true")
    .getOrCreate()
)

# GCS and BigQuery configurations
gcs_file_path = "gs://test-composer-sample-spark/data/sample_data.csv"
temporary_gcs_bucket = "temp-composer-sample-spark"
bq_table = "arcane-world-420020.sample.sample_spark_table"

try:
    # Read the CSV file from GCS into a DataFrame
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(gcs_file_path)
    )

    # Show data for debugging
    df.show()

    # Write the DataFrame to BigQuery
    df.write.format("bigquery").mode("overwrite").save(bq_table)

    print("Data successfully written to BigQuery!")

except AnalysisException as e:
    print(f"AnalysisException: {e}")
except Exception as e:
    print(f"Error: {e}")
finally:
    spark.stop()
