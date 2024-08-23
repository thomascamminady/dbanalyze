import duckdb
import os

# Define the absolute path where all the partitioned files are located
ROOT = "../../HealthData/FitFiles/Parquet/"
# Connect to DuckDB
con = duckdb.connect()

# Define the path for the output summary file
summary_file = os.path.join(ROOT, "summary.parquet")

# Generate the SQL query to read and merge all parquet files with Hive partitioning
sql = f"""
copy (
   	select 
		* 
	from 
		read_parquet(
			'{ROOT}hive/year=*/month=*/*.parquet', 
			hive_partitioning = true, 
			union_by_name = true
		)
) 
	to '{summary_file}' (format parquet);
"""

# Execute the SQL command to merge and write the summary file
con.execute(sql)

