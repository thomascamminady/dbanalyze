import polars as pl
import glob
import os.path

if __name__ == "__main__":
    path = "../../HealthData/FitFiles/Activities/"
    files = glob.glob(path + "*details*.json")
    for file in files:
        _ = (
            pl.read_json(file)
            .select(
                "activityId",
                "activityName",
                "summaryDTO",
                "activityTypeDTO",
            )
            .unnest("summaryDTO", "activityTypeDTO")
            # .write_ndjson(outfile)
        )
        suffix = _["startTimeGMT"].to_list()[0]
        outfile = f"../../HealthData/FitFiles/Parquet/{suffix}.parquet"
        _.write_parquet(outfile)
# pl.concat(
#     [
#         pl.scan_parquet(f)
#         for f in glob.glob("../../HealthData/FitFiles/Parquet/*.parquet")
#     ],
#     how="diagonal_relaxed",
# ).sink_parquet("../../HealthData/FitFiles/Activities.parquet")
