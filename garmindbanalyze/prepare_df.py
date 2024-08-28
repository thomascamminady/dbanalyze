import time

import numpy as np
import polars as pl
import polars.selectors as cs


def drop_columns_that_are_all_null(_df: pl.DataFrame) -> pl.DataFrame:
    return _df[[s.name for s in _df if not (s.null_count() == _df.height)]]


def cast_to_pace_numeric(_):
    try:
        return _ / 60
    except Exception:
        return np.inf


def cast_to_pace(_):
    try:
        return f"{int(_//60)}:{int(_%60):02d}"
    except Exception:
        return "00:00"


def prepare_df(file: str, head: int | None = None) -> pl.DataFrame:
    df = (
        pl.read_parquet(file)
        .sort("startTimeGMT", descending=True)
        .fill_nan(0.0)
        .fill_null(0.0)
        .pipe(drop_columns_that_are_all_null)
        .with_columns(pl.col("startTimeGMT").cast(pl.Datetime))
        .with_columns(
            date=pl.col("startTimeGMT").dt.date(),
            year=pl.col("startTimeGMT").dt.year(),
            week=pl.col("startTimeGMT").dt.week(),
            time=pl.col("startTimeGMT").dt.time(),
            day_of_week=pl.col("startTimeGMT").dt.weekday(),
        )
        .with_columns(
            pl.col("day_of_week").map_elements(
                lambda _: {
                    1: "Monday",
                    2: "Tuesday",
                    3: "Wednesday",
                    4: "Thursday",
                    5: "Friday",
                    6: "Saturday",
                    7: "Sunday",
                }[_][:3],
                return_dtype=pl.Utf8,
            )
        )
        .with_columns(
            year_week=pl.col("year").cast(pl.Utf8)
            + pl.lit("-")
            + pl.col("week").cast(pl.Utf8)
        )
        .with_columns(pl.col("distance") / 1_000)
        .with_columns(
            average_moving_pace_sec=pl.col("movingDuration")
            / pl.col("distance")
        )
        .with_columns(
            max_pace=1000 / pl.col("maxSpeed")
        )  # speed is Meters / Second
        .fill_null(0.0)
        .fill_nan(0.0)
        .with_columns(
            average_moving_pace_numeric=pl.col(
                "average_moving_pace_sec"
            ).map_elements(cast_to_pace_numeric, return_dtype=pl.Float64),
            max_pace_numeric=pl.col("max_pace").map_elements(
                cast_to_pace_numeric, return_dtype=pl.Float64
            ),
            average_moving_pace=pl.col("average_moving_pace_sec").map_elements(
                cast_to_pace, return_dtype=pl.Utf8
            ),
            max_pace=pl.col("max_pace").map_elements(
                cast_to_pace, return_dtype=pl.Utf8
            ),
        )
        .with_columns(
            pl.col("duration", "movingDuration").map_elements(
                lambda _: time.strftime("%H:%M:%S", time.gmtime(_)),
                return_dtype=pl.Utf8,
            )
        )
        .with_columns(
            pl.col("trainingEffectLabel")
            .str.replace_all("_", " ")
            .str.to_titlecase()
        )
        .with_columns(pl.col("activityName").str.replace(" Running", ""))
        .with_columns(pl.col("activityName").str.replace_all("ö", "oe"))
        .with_columns(pl.col("activityName").str.replace_all("ä", "ae"))
        .with_columns(pl.col("activityName").str.replace_all("ü", "ue"))
        .with_columns(pl.col("activityName").str.replace_all("ß", "ss"))
        .with_columns(
            url=pl.lit("https://connect.garmin.com/modern/activity/")
            + pl.col("activityId").cast(pl.Int64).cast(pl.Utf8)
        )
        .with_columns(
            activityName=pl.lit("[")
            + pl.col("activityName")
            + pl.lit("]")
            + pl.lit("(")
            + pl.col("url")
            + pl.lit(")")
        )
        .with_columns(cs.numeric().round(3))
    )

    if head is not None:
        df = df.head(head)

    df = (
        pl.concat(
            [
                df.filter(pl.col("typeKey").is_in(["running", "track_running"]))
                .group_by("year_week", maintain_order=True)
                .agg(
                    pl.sum(
                        "distance",
                        "elevationLoss",
                        "elevationGain",
                        "trainingEffect",
                        "anaerobicTrainingEffect",
                        "activityTrainingLoad",
                        "calories",
                    ).round(1),
                    pl.col("date").max(),
                ),
                df,
            ],
            how="diagonal_relaxed",
        )
        .with_columns(
            pl.col("typeKey")
            .str.replace("running", "run")
            .str.replace("_", " ")
            .str.to_titlecase()
        )
        .select(
            "year_week",
            "day_of_week",
            "typeKey",
            "activityName",
            "time",
            "date",
            "distance",
            "movingDuration",
            "average_moving_pace",
            "average_moving_pace_numeric",
            "averageRunCadence",
            "maxRunCadence",
            "trainingEffectLabel",
            "trainingEffect",
            "anaerobicTrainingEffect",
            "activityTrainingLoad",
            "directWorkoutRpe",
            "elevationGain",
            "elevationLoss",
            "averageHR",
            "maxHR",
            "minTemperature",
            "averageTemperature",
            "maxTemperature",
            "calories",
        )
    )
    return df
