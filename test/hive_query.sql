-- Create a table if it doesn't exist
CREATE TABLE IF NOT EXISTS sample_db.sample_table (
    column1 STRING,
    column2 INT
);

-- Insert sample data
INSERT INTO TABLE sample_db.sample_table VALUES ('A', 1), ('B', 2), ('A', 3), ('B', 4);

-- Query to process the data
SELECT column1, COUNT(*) AS count
FROM sample_db.sample_table
GROUP BY column1;
