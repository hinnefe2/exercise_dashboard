import datetime
import os

import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objects as go

from plotly.subplots import make_subplots
from sqlalchemy import create_engine


external_stylesheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

engine = create_engine(
    "postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}:5432/postgres".format(**os.environ)
)


def _build_activity_heatmap(timeseries_data):

    d1 = timeseries_data["date"].min().date()
    d2 = timeseries_data["date"].max().date()

    delta = d2 - d1

    # gives me a list with datetimes for each day a year
    dates_in_year = [d1 + datetime.timedelta(i) for i in range(delta.days + 1)]

    joined = (
        pd.Series(dates_in_year)
        .to_frame()
        .set_index(0)
        .join(timeseries_data.set_index("date"))
    )

    # the activity values to actually plot in the heatmap
    # dates when the joined data isn't null are days when
    # there were activity
    z = (~joined.isna()).astype(int)["values"].values

    # gives something like list of strings like '2018-01-25' for each date.
    # Used in data trace to make good hovertext.
    text = [str(i) for i in dates_in_year]

    # 4cc417 green #347c17 dark green
    colorscale = [[False, "#eeeeee"], [True, "#76cf63"]]

    trace = go.Heatmap(
        # horizontally index on the most recent monday
        # day - day.weekday() gives the most recent monday
        x=[(day - datetime.timedelta(days=day.weekday())) for day in dates_in_year],
        # vertically index on the day of week for each date
        y=[day.weekday() for day in dates_in_year],
        z=z,
        text=text,
        hoverinfo="text",
        xgap=3,  # this
        ygap=3,  # and this is used to make the grid-like apperance
        showscale=False,
        colorscale=colorscale,
    )

    layout = go.Layout(
        height=280,
        yaxis=dict(
            showline=False,
            showgrid=False,
            zeroline=False,
            tickmode="array",
            ticktext=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
            tickvals=[0, 1, 2, 3, 4, 5, 6],
        ),
        xaxis=dict(
            showline=False,
            showgrid=False,
            zeroline=False,
        ),
        font={"size": 10, "color": "#9e9e9e"},
        plot_bgcolor=("#fff"),
        margin=dict(t=40),
    )

    return trace, layout


def build_activity_indicator(timeseries_data, indicator_value, indicator_name):
    """Build a activity log + indicator figure."""

    fig = make_subplots(
        rows=1,
        cols=2,
        column_widths=[0.7, 0.3],
        specs=[[{"type": "heatmap"}, {"type": "indicator"}]],
    )

    trace, layout = _build_activity_heatmap(timeseries_data)

    # add the timeseries scatter plot
    fig.add_trace(
        trace,
        row=1,
        col=1,
    )

    # add summary statistic "indicator"
    fig.add_trace(
        go.Indicator(
            mode="number",
            value=indicator_value,
            name=indicator_name,
        ),
        row=1,
        col=2,
    )

    fig.update_layout(layout)
    fig.update_layout(
        template={"data": {"indicator": [{"title": {"text": indicator_name}}]}},
    )

    return fig


def build_timeseries_indicator(timeseries_data, indicator_value, indicator_name):
    """Build a timeseries + indicator figure."""

    fig = make_subplots(
        rows=1,
        cols=2,
        column_widths=[0.7, 0.3],
        specs=[[{"type": "xy"}, {"type": "indicator"}]],
    )

    # add the timeseries scatter plot
    fig.add_trace(
        go.Scatter(
            mode="markers", x=timeseries_data["date"], y=timeseries_data["values"]
        ),
        row=1,
        col=1,
    )
    # add the rolling average timeseries line plot
    rolling = (
        timeseries_data.set_index("date")
        .rolling(4, win_type="triang", center=True)
        .mean()
        .dropna()
        .reset_index()
    )
    fig.add_trace(
        go.Scatter(mode="lines", x=rolling["date"], y=rolling["values"]),
        row=1,
        col=1,
    )

    # add summary statistic "indicator"
    fig.add_trace(
        go.Indicator(
            mode="number",
            value=indicator_value,
            name=indicator_name,
        ),
        row=1,
        col=2,
    )

    fig.update_layout(
        template={"data": {"indicator": [{"title": {"text": indicator_name}}]}},
    )

    return fig


def prep_calorie_data(engine, start_date):
    """Read and process calorie data.

    Returns
    -------
    pd.DataFrame
        A dataframe having columns ['date', 'values']
    float
        A scalar value
    """

    # TODO: move the ETL into dbt
    mfp_data = pd.read_sql("SELECT * FROM myfitnesspal.totals", engine)
    fitbit_data = pd.read_sql("SELECT * FROM fitbit.activity", engine)

    fitbit_data = fitbit_data.set_index(pd.to_datetime(fitbit_data["date"]))[
        ["calories_out"]
    ]
    fitbit_data = fitbit_data.loc[fitbit_data["calories_out"] != 0]
    mfp_data = mfp_data.groupby("date").sum()[["calories"]]
    mfp_data = mfp_data.loc[mfp_data["calories"] != 0]

    joined = mfp_data.join(fitbit_data, how="inner")
    joined = joined[start_date:]
    joined["values"] = joined.calories - joined.calories_out

    # indicator for calories is mean delta over past seven days
    today = datetime.date.today()
    week_ago = today - datetime.timedelta(weeks=1)

    return (
        joined["values"].reset_index(),
        joined.loc[week_ago.isoformat():, "values"].mean(),
    )


def prep_weight_data(engine, start_date):
    """Read and process weight data.

    Returns
    -------
    pd.DataFrame
        A dataframe having columns ['date', 'values']
    float
        A scalar value
    """

    # TODO: move the ETL into dbt
    # including casting 'date' to a datetime column in postgres
    # including back/forward filling (or interpolating)
    weight_data = pd.read_sql("SELECT * FROM fitbit.weight", engine)
    weight_data = weight_data.set_index(pd.to_datetime(weight_data["date"]))
    weight_data = weight_data.sort_index()
    weight_data = weight_data.fillna(method="ffill")
    weight_data = weight_data[start_date:]

    # indicator for weight is difference in 7 day rolling means
    # now versus a week ago
    today = datetime.date.today()
    latest = weight_data.index.max()
    week_ago = today - datetime.timedelta(weeks=1)

    rolling = weight_data.rolling(7).mean()

    indicator = (
        rolling.loc[latest.isoformat(), "weight"]
        - rolling.loc[week_ago.isoformat(), "weight"]
    )

    weight_data = weight_data["weight"].reset_index()
    weight_data.columns = ["date", "values"]

    return weight_data, indicator


def prep_activity_data(engine, start_date):
    """Read and process exercise activity data.

    Returns
    -------
    pd.DataFrame
        A dataframe having columns ['date', 'values']
    float
        A scalar value
    """

    # TODO: move the ETL into dbt
    # including casting 'date' to a datetime column in postgres
    # including back/forward filling (or interpolating)
    activity_data = pd.read_sql("SELECT * FROM googlefit.sessions", engine)
    activity_data = activity_data[["date", "name"]]
    activity_data["date"] = pd.to_datetime(activity_data["date"])

    activity_data.columns = ["date", "values"]

    return activity_data, 0


calorie_ts, calorie_indicator = prep_calorie_data(
    engine, datetime.date.fromisoformat("2021-02-09")
)

weight_ts, weight_indicator = prep_weight_data(
    engine, datetime.date.fromisoformat("2021-02-01")
)

activity_ts, activity_indicator = prep_activity_data(
    engine, datetime.date.fromisoformat("2021-02-01")
)

app.layout = html.Div(
    children=[
        dcc.Graph(
            id="exercise-fig",
            figure=build_activity_indicator(
                activity_ts, activity_indicator, "Current Streak"
            ),
        ),
        dcc.Graph(
            id="weight-fig",
            figure=build_timeseries_indicator(
                weight_ts, weight_indicator, "Weekly Change"
            ),
        ),
        dcc.Graph(
            id="calories-fig",
            figure=build_timeseries_indicator(
                calorie_ts, calorie_indicator, "Weekly Avg"
            ),
        ),
    ]
)

if __name__ == "__main__":
    app.run_server(debug=True)
