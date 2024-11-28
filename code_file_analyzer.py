import os
import re
import pandas as pd
import ast
from croniter import croniter
from pathlib import Path
import sqlparse

FILE_TYPES = {
    ".sh": "Shell Script",
    ".py": "Python Script",
    ".sql": "SQL Script",
}

# List of SQL keywords to detect
SQL_KEYWORDS = r"(SELECT |INSERT TABLE|UPDATE |DELETE FROM|CREATE TABLE|DROP |ALTER TABLE|TRUNCATE TABLE)"


def count_lines(file_path):
    """Count the number of lines in a file."""
    return sum(1 for _ in open(file_path, encoding="utf-8", errors="ignore"))


def extract_sql_queries(content):
    """Extract and validate SQL queries using sqlparse."""
    sql_queries = []
    potential_queries = re.findall(r"['\"](.*?['\"])", content, re.DOTALL)

    for query in potential_queries:
        if re.search(SQL_KEYWORDS, query, re.IGNORECASE):
            parsed = sqlparse.parse(query)
            if parsed:
                sql_queries.append(query.strip())
    return sql_queries


def extract_cron_triggers(file_path):
    """Extract cron triggers from shell scripts."""
    cron_triggers = []

    # Read the shell script content
    with open(file_path, encoding="utf-8", errors="ignore") as f:
        content = f.read()

        # Check for direct cron expressions in the script
        cron_match = re.search(r'CRON_SCHEDULE\s*=\s*"([0-9\s\*\/\-\,\?]+)"', content)
        if cron_match:
            cron_schedule = cron_match.group(1)
            # Validate if the cron expression is valid
            if croniter.is_valid(cron_schedule):
                cron_triggers.append(cron_schedule)
                print(f"Detected cron schedule: {cron_schedule}")

        # Optionally check for other cron job lines (e.g., crontab entries) if applicable
        # Assuming crontab lines may also be embedded, this is another approach to capture cron expressions
        for line in content.splitlines():
            if re.match(r"^\s*\d+\s+\d+\s+\d+\s+\d+\s+\d+", line):
                cron_expr = line.strip().split(" ")[0:5]
                cron_expr = " ".join(cron_expr)
                if croniter.is_valid(cron_expr):
                    cron_triggers.append(cron_expr)
                    print(f"Detected cron expression: {cron_expr}")

    return cron_triggers


def analyze_pyspark(file_path):
    """Analyze PySpark scripts for data sources and sinks, including GCS, Hive, BigQuery, CSV, Parquet, and JDBC."""
    sources, sinks, dependencies = [], [], []
    variables = {}
    query_count = 0  # To count the number of SQL queries executed
    with open(file_path, encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()
        content = f.read()
        # Updated regex to handle multi-line SQL queries within spark.sql()
        sql_query_pattern = re.compile(
            r'\s*spark\.sql\([\'"](.*?)[\'"]\)', re.DOTALL
        )  # Matches across lines

        sql_queries = []  # To store the matched queries
        # Detect SQL strings by using sqlparse to validate and extract queries
        string_pattern = (
            r"[\"']([^\"']*?[SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER].*?)[\"']"
        )
        potential_queries = re.findall(string_pattern, content, re.IGNORECASE)

        for query in potential_queries:
            parsed = sqlparse.parse(query)
            if parsed:
                sql_queries.append(query.strip())
                query_count += 1
        print(
            f"------------------------------------------PYSPARK----query_count {query_count}"
        )
        # Iterate through each line to detect sources, sinks, and SQL queries
        for line in lines:
            # Detect dependencies: look for PySpark imports
            if line.startswith("from pyspark.") or line.startswith("import pyspark"):
                dependencies.append(line.strip())
            elif "import" in line:  # General Python import
                dependencies.append(line.strip())

            # Track variable assignments (e.g., paths, table names, buckets)
            var_match = re.match(r"(\w+)\s*=\s*[\"'](.+?)[\"']", line)
            if var_match:
                var_name, var_value = var_match.group(1), var_match.group(2)
                variables[var_name] = var_value
                print(f"Detected variable: {var_name} = {var_value}")

            # Detect sources from `.load()` and `.read.table()` (for Hive and other data sources)
            if "spark.read" in line:
                # Handle `.table()` calls (for Hive tables)
                table_match = re.search(r"\.read\.table\(\s*([\"'])(.*?)\1\s*\)", line)
                if table_match:
                    table_name = table_match.group(2).strip()
                    # Check if the table name refers to a variable
                    if table_name in variables:
                        resolved_table = variables[table_name]
                        sources.append(f"Hive Table: {resolved_table}")
                        print(f"Source found via variable: Hive Table {resolved_table}")
                    else:
                        sources.append(f"Hive Table: {table_name}")
                        print(f"Source found: Hive Table {table_name}")

                # Handle `.load()` calls for other sources (e.g., Parquet, CSV)
                load_match = re.search(r"\.load\(\s*([\"'].*?[\"']|\w+)\s*\)", line)
                if load_match:
                    load_arg = load_match.group(1).strip()
                    load_arg = load_arg.strip("\"'")

                    if load_arg in variables:
                        resolved_path = variables[load_arg]
                        sources.append(resolved_path)
                        print(f"Source found via variable: {resolved_path}")
                    elif load_arg.startswith(("gs://", "hdfs://")) or load_arg.endswith(
                        (".csv", ".parquet", ".json")
                    ):
                        sources.append(load_arg)
                        print(f"Source found via direct path: {load_arg}")

            # Detect sinks from `.write` and `.save()` (for data sinks)
            if ".write" in line:
                # Handle `.saveAsTable()` calls (Hive table sinks)
                save_table_match = re.search(
                    r"\.write\.mode\([\"'](\w+)[\"']\)\.saveAsTable\(\s*[\"'](.*?)['\"]\s*\)",
                    line,
                )
                if save_table_match:
                    save_mode = save_table_match.group(1)
                    table_name = save_table_match.group(2).strip()
                    sinks.append(f"Hive Table: {table_name} (mode: {save_mode})")
                    print(f"Sink found: Hive Table {table_name} (mode: {save_mode})")

                # Handle `.parquet()` calls (for writing to a Parquet file)
                save_parquet_match = re.search(
                    r"\.write\.mode\([\"'](\w+)[\"']\)\.parquet\(\s*[\"'](.*?)['\"]\s*\)",
                    line,
                )
                if save_parquet_match:
                    save_mode = save_parquet_match.group(1)
                    file_path = save_parquet_match.group(2).strip()
                    sinks.append(f"Parquet File: {file_path} (mode: {save_mode})")
                    print(f"Sink found: Parquet File {file_path} (mode: {save_mode})")

            # Detect SQL queries via `spark.sql("<SQL_QUERY>")`
            sql_query_match = sql_query_pattern.search(line)
            if sql_query_match:
                sql_query = sql_query_match.group(1).strip()
                # Check if the query contains any SQL keywords
                if re.search(SQL_KEYWORDS, sql_query, re.IGNORECASE):
                    query_count += 1  # Increment query count
                    sql_queries.append(
                        sql_query
                    )  # Store the SQL query for later debugging
                    print(f"SQL query detected: {sql_query}")

    # Final debugging outputs
    print("Final sources found:", sources)
    print("Final sinks found:", sinks)
    print(f"Detected dependencies in PySpark job: {dependencies}")
    print(f"Total SQL queries detected: {query_count}")
    print(f"SQL Queries detected: {sql_queries}")

    return sources, sinks, dependencies, query_count


# Helper function to extract import statements
def extract_imports(file_path):
    """Extract all import dependencies from Python or PySpark scripts."""
    imports = []
    with open(file_path, encoding="utf-8", errors="ignore") as f:
        tree = ast.parse(f.read(), filename=file_path)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend([alias.name for alias in node.names])
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                imports.append(f"{module}.{node.names[0].name}")
    return imports


def analyze_python_file(file_path):
    """Analyze Python scripts (non-PySpark) for dependencies."""
    dependencies = extract_imports(file_path)
    print(f"Detected Python dependencies: {dependencies}")
    return dependencies


def extract_variables(file_path):
    """Extract variables and their counts from a file."""
    variables = {}
    variable_counts = {}

    with open(file_path, encoding="utf-8", errors="ignore") as f:
        for line in f:
            # Detect simple variable assignments (e.g., VAR=value or var = "value")
            var_match = re.match(
                r"(\w+)\s*=\s*[\"']?(.+?)[\"']?\s*(?:#.*)?$", line.strip()
            )
            if var_match:
                var_name, var_value = var_match.group(1), var_match.group(2)
                variables[var_name] = var_value
                variable_counts[var_name] = variable_counts.get(var_name, 0) + 1
                print(f"Detected variable: {var_name} = {var_value}")

    return variables, variable_counts


def extract_function_and_class_names(file_path):
    """Extract function and class names from a Python script."""
    class_names = []
    function_names = []

    with open(file_path, encoding="utf-8", errors="ignore") as f:
        tree = ast.parse(f.read(), filename=file_path)
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                class_names.append(node.name)
            elif isinstance(node, ast.FunctionDef):
                function_names.append(node.name)

    return class_names, function_names


def extract_imports_and_counts(file_path):
    """Extract imports, class count, and function count from Python files."""
    imports = []
    class_count = 0
    function_count = 0
    with open(file_path, encoding="utf-8", errors="ignore") as f:
        tree = ast.parse(f.read(), filename=file_path)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend([alias.name for alias in node.names])
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                imports.append(f"{module}.{node.names[0].name}")
            elif isinstance(node, ast.ClassDef):
                class_count += 1
            elif isinstance(node, ast.FunctionDef):
                function_count += 1
    return imports, class_count, function_count


# def analyze_sql(file_path):
#     """Analyze SQL scripts for tables and data sources."""
#     sources, sinks = [], []
#     with open(file_path, encoding="utf-8", errors="ignore") as f:
#         content = f.read()
#         table_matches = re.findall(r"(FROM|INTO)\s+([`'\"].+?[`'\"].+?)\s*", content)
#         for match in table_matches:
#             action, table = match
#             if action.upper() == "FROM":
#                 sources.append(table)
#                 print(f"Detected source table: {table}")
#             elif action.upper() == "INTO":
#                 sinks.append(table)
#                 print(f"Detected sink table: {table}")
#     return sources, sinks


def analyze_sql(file_path):
    """Analyze SQL scripts for tables and query counts."""
    tables = []
    query_count = 0
    with open(file_path, encoding="utf-8", errors="ignore") as f:
        content = f.read()
        matches = re.findall(r"FROM\s+(\w+)|JOIN\s+(\w+)", content, re.IGNORECASE)
        tables = [tbl for pair in matches for tbl in pair if tbl]
        query_count = len(
            re.findall(r"\b(SELECT|INSERT|UPDATE|DELETE)\b", content, re.IGNORECASE)
        )
    return tables, query_count


def analyze_file(file_path, repo_name):
    """Analyze a single file for its properties."""
    ext = Path(file_path).suffix
    file_type = FILE_TYPES.get(ext, "Unknown")

    file_info = {
        "source": repo_name,
        "file_name": Path(file_path).name,
        "type": file_type,
        "lines_of_code": count_lines(file_path),
        "class_count": 0,
        "function_count": 0,
        "query_count": 0,
        "purpose": "",
        "dependencies": [],
        "triggers": [],
        "sources": [],  # Initialize sources as an empty list
        "sinks": [],  # Initialize sinks as an empty list
        # "variables": {},
        # "variable_counts": {},
        "imports": [],
        "function_names": [],
        "class_names": [],
    }

    # Extract variables and counts
    # variables, variable_counts = extract_variables(file_path)
    # file_info["variables"] = variables
    # file_info["variable_counts"] = variable_counts

    if file_type == "Shell Script":
        file_info["triggers"] = extract_cron_triggers(file_path)

    elif file_type == "Python Script":
        # Analyze dependencies and functions for Python files
        file_info["dependencies"] = analyze_python_file(file_path)
        imports, class_count, function_count = extract_imports_and_counts(file_path)
        file_info["imports"] = imports
        file_info["class_count"] = class_count
        file_info["function_count"] = function_count
        class_names, function_names = extract_function_and_class_names(file_path)
        file_info["function_names"] = function_names
        file_info["class_names"] = class_names
        with open(file_path, encoding="utf-8", errors="ignore") as f:
            content = f.read()
            sql_queries = extract_sql_queries(content)
            file_info["query_count"] = len(sql_queries)
        if "pyspark" in " ".join(imports):
            file_info["purpose"] = "PySpark Job"
            sources, sinks, dependencies, query_count = analyze_pyspark(
                file_path
            )  # Extract sources and sinks
            file_info["sources"] = sources  # Assign sources
            file_info["sinks"] = sinks  # Assign sinks
            file_info["dependencies"] = dependencies
            file_info["query_count"] = len(sql_queries)

    elif file_type == "SQL Script":
        file_info["purpose"] = "SQL Query"
        dependencies, query_count = analyze_sql(file_path)
        file_info["dependencies"] = dependencies
        file_info["query_count"] = query_count

    return file_info


def analyze_repo(repo_path, output_csv, repo_name):
    """Analyze a repository and save results as a CSV."""
    results = []

    for root, _, files in os.walk(repo_path):
        for file in files:
            file_path = os.path.join(root, file)
            if Path(file_path).suffix in FILE_TYPES:
                file_info = analyze_file(file_path, repo_name)
                # # Serialize variables and counts for CSV
                # file_info["variables"] = ", ".join(
                #     file_info["variables"].keys()
                # )  # List of variable names
                # file_info["variable_counts"] = len(
                #     file_info["variable_counts"]
                # )  # Total number of variables

                results.append(file_info)

    # Convert results to a DataFrame for output
    df = pd.DataFrame(results)
    df.to_csv(output_csv, index=False)
    print(f"Analysis complete. Results saved to {output_csv}")


# Main Execution
if __name__ == "__main__":
    repo_path = input("Enter the path to the code repository: ").strip()
    repo_name = Path(repo_path).name
    output_csv = "repo_analysis.csv"
    analyze_repo(repo_path, output_csv, repo_name)
