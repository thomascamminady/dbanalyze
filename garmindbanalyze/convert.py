import duckdb
import os
import glob

# Define the absolute path where all this should happen
ROOT = "../../HealthData/FitFiles/Parquet/"

# Connect to DuckDB
con = duckdb.connect()

# Define the years and months you want to loop over
years = range(2016, 2025)
months = range(1, 13)  # Months from January (1) to December (12)

# Loop through the years and months and generate the SQL command
for year in years:
    for month in months:
        # Format the month as two digits
        month_str = f"{month:02}"

        # Define the input pattern path within the ROOT directory
        input_pattern = os.path.join(ROOT, f"{year}-{month_str}*.parquet")

        if len(glob.glob(input_pattern)) == 0:
            continue

        # Define the output directory in Hive-style partitioning using the absolute path
        output_dir = os.path.join(ROOT, f"hive/year={year}/month={month_str}")

        # Ensure the directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Define the output file path
        output_file = os.path.join(output_dir, "data.parquet")

        # Generate the SQL command
        sql = f"""
        copy(
            select * from read_parquet('{input_pattern}', union_by_name=True, filename=True)
        ) to '{output_file}' (format parquet);
        """

        # Execute the SQL command
        con.execute(sql)
