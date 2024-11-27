from pyspark.sql import SparkSession


class PySparkHiveExample:
    def __init__(self, app_name="PySpark Hive SQL Example"):
        """Initialize the Spark session with Hive support."""
        self.spark = (
            SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
        )

    def load_data_from_hive(self, hive_table):
        """Load data from a Hive table into a DataFrame."""
        query = f"SELECT * FROM {hive_table}"
        df = self.spark.sql(query)
        return df

    def create_result_table(self):
        """Create a result table in Hive by performing a SQL query."""
        self.spark.sql(
            """
            CREATE TABLE IF NOT EXISTS sample_db.result_table AS
            SELECT column1, COUNT(*) as count
            FROM sample_db.sample_table
            GROUP BY column1
        """
        )

    def show_data(self, df):
        """Show the data in a DataFrame."""
        df.show()

    def stop_spark_session(self):
        """Stop the Spark session."""
        self.spark.stop()


# Main execution
if __name__ == "__main__":
    # Initialize the PySparkHiveExample class
    pyspark_job = PySparkHiveExample()

    # Load data from a Hive table
    hive_table = "sample_db.sample_table"
    df = pyspark_job.load_data_from_hive(hive_table)

    # Show the loaded data for debugging
    pyspark_job.show_data(df)

    # Create a result table in Hive
    pyspark_job.create_result_table()

    # Verify the result table creation
    result_df = pyspark_job.load_data_from_hive("sample_db.result_table")
    pyspark_job.show_data(result_df)

    # Stop the Spark session
    pyspark_job.stop_spark_session()
