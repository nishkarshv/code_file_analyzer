#!/bin/bash

# Set environment variables for Hadoop, Hive, and Spark if not already set
export HADOOP_HOME=/path/to/hadoop
export SPARK_HOME=/path/to/spark
export HIVE_HOME=/path/to/hive

# Path to the PySpark script
SPARK_SCRIPT="/path/to/your/spark_job.py"

# Log file to capture output
LOG_FILE="/path/to/logs/spark_job.log"

# Cron schedule: Every day at 2:00 AM
CRON_SCHEDULE="0 2 * * *"

# Cron job to run the shell script at the specified time
CRON_JOB="$CRON_SCHEDULE /bin/bash $0"

# Function to set up the cron job
setup_cron() {
    (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
    echo "Cron job set up successfully to run at $CRON_SCHEDULE"
}

# Function to run the PySpark job
run_pyspark_job() {
    # Start logging
    echo "Starting PySpark job at $(date)" >> $LOG_FILE

    # Run the PySpark job using spark-submit
    $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster $SPARK_SCRIPT >> $LOG_FILE 2>&1

    # Check if the job was successful or failed
    if [ $? -eq 0 ]; then
        echo "PySpark job completed successfully at $(date)" >> $LOG_FILE
    else
        echo "PySpark job failed at $(date)" >> $LOG_FILE
    fi
}

# Main execution
# If this script is executed for the first time, it sets up the cron job
if [ "$1" != "run" ]; then
    setup_cron
    echo "Cron job setup complete. Script will run as per the scheduled time."
    exit 0
fi

# If the script is triggered by cron, it runs the PySpark job
run_pyspark_job
