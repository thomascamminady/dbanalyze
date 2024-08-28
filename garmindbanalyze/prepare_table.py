import datetime

import polars as pl
import polars.selectors as cs
from great_tables import GT, loc, style


def prepare_table(df):
    return (
        GT(df)
        .tab_header(
            title="Activity Overview",
            subtitle=f"""Last updated: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}""",
        )
        .tab_stub(rowname_col="date", groupname_col="year_week")
        .tab_spanner(
            label="Elevation", columns=["elevationLoss", "elevationGain"]
        )
        .tab_spanner(label="Heart Rate", columns=["averageHR", "maxHR"])
        .tab_spanner(
            label="Training Effect",
            columns=[
                "trainingEffect",
                "anaerobicTrainingEffect",
                "trainingEffectLabel",
                "activityTrainingLoad",
                "directWorkoutRpe",
            ],
        )
        .tab_spanner(
            label="Temperature",
            columns=["minTemperature", "averageTemperature", "maxTemperature"],
        )
        .tab_spanner(
            label="Cadence", columns=["averageRunCadence", "maxRunCadence"]
        )
        .tab_spanner(
            label="Pace",
            columns=["average_moving_pace", "average_moving_pace_numeric"],
        )
        .fmt_number(
            columns="distance", compact=True, pattern="{x} km", n_sigfig=3
        )
        .fmt_number(
            columns=[
                "averageTemperature",
            ],
            compact=True,
            pattern="{x}",
            n_sigfig=1,
        )
        .fmt_number(
            columns=["average_moving_pace_numeric"],
            compact=True,
            pattern="{x}",
            n_sigfig=2,
        )
        .fmt_number(
            columns=["activityTrainingLoad"],
            compact=True,
            pattern="{x}",
            n_sigfig=3,
        )
        .fmt_number(
            columns="averageRunCadence", compact=True, pattern="{x}", n_sigfig=3
        )
        .fmt_number(
            columns="maxRunCadence", compact=True, pattern="{x}", n_sigfig=3
        )
        .fmt_date(columns="date", date_style="day_m")
        .fmt_time(columns="time", time_style="h_m_p")
        .data_color(
            columns=["anaerobicTrainingEffect", "trainingEffect"],
            palette=["white", "purple"],
            domain=[0, 5],
            na_color="white",
        )
        .data_color(
            columns=["distance"],
            palette=["white", "blue"],
            domain=[0, 42],
            na_color="white",
        )
        .data_color(
            columns=["average_moving_pace_numeric"],
            palette=["white", "gold"],
            domain=[6.5, 3.5],
            na_color="white",
        )
        .data_color(
            columns=["activityTrainingLoad"],
            palette=["white", "blue"],
            domain=[0, 400],
            na_color="white",
        )
        .data_color(
            columns=["averageHR"],
            palette=["white", "red"],
            domain=[120, 190],
            na_color="white",
        )
        .data_color(
            columns=["elevationGain", "elevationLoss"],
            palette=["white", "green"],
            domain=[0, 500],
            na_color="white",
        )
        .fmt_markdown(columns=["activityName"])
        .cols_label(
            day_of_week=" ",
            time="Time",
            movingDuration="Duration",
            distance="Distance",
            trainingEffectLabel="Label",
            trainingEffect="Overall",
            anaerobicTrainingEffect="Anaerobic",
            average_moving_pace="Average",
            average_moving_pace_numeric="Numeric",
            averageRunCadence="Average",
            maxRunCadence="Max",
            activityTrainingLoad="Load",
            directWorkoutRpe="RPE",
            minTemperature="Min",
            averageTemperature="Average",
            maxTemperature="Max",
            calories="kCal",
            elevationLoss="Loss",
            elevationGain="Gain",
            averageHR="Average",
            maxHR="Max",
            activityName="Activity",
            typeKey="Type",
        )
        .cols_align("left")
        .cols_align("left", columns="activityName")
        .opt_align_table_header(align="left")
        .opt_vertical_padding(1)
        .tab_source_note(source_note="")
        .sub_missing(missing_text="")
        .tab_options(
            # table_body_vlines_color="red",
            # table_body_vlines_style="dashed",
            # table_body_vlines_width="4px",
            stub_background_color="#eeeeee",
            stub_border_color="#eeeeee",
        )
        .tab_style(
            [
                style.fill(color="#eeeeee"),
                style.text(weight="bold", color="black"),
            ],
            loc.body(
                columns=cs.all(),
                rows=(
                    (pl.col("date") == pl.col("date").first().over("year_week"))
                    & (pl.col("time").is_null())
                ),
            ),
        )
        .tab_style(
            [style.fill(color="#dcfcfc")],
            loc.body(
                columns=pl.col("activityName", "typeKey"),
                rows=(pl.col("typeKey") == "Hiking"),
            ),
        )
        .tab_style(
            [style.fill(color="#fcdcfc")],
            loc.body(
                columns=pl.col("activityName", "typeKey"),
                rows=(pl.col("typeKey") == "Track Run"),
            ),
        )
        .tab_style(
            [style.fill(color="#fcfcdc")],
            loc.body(
                columns=pl.col("activityName", "typeKey"),
                rows=(pl.col("typeKey") == "Cycling"),
            ),
        )
        # .tab_style(
        #     [style.fill(color="#ffdddd")],
        #     loc.body(
        #         columns="activityName",
        #         rows=pl.col("activityName").str.contains("Track"),
        #     ),
        # )
        .tab_options(table_width="100%")
    )
