from prepare_df import prepare_df
from prepare_table import prepare_table

if __name__ == "__main__":
    path = "/home/thomas/HealthData/FitFiles/Parquet/summary.parquet"
    df = prepare_df(path, 100)

    table = prepare_table(df)
    with open("/home/thomas/HealthData/Activities.html", "w") as f:
        f.write(table.as_raw_html())
