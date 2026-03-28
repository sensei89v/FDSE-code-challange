import marimo

__generated_with = "0.7.0"
app = marimo.App(width="full", app_title="Rig Live Metrics")


@app.cell
def setup_cell():
    import marimo
    import pandas
    import psycopg2
    from plotly import graph_objects
    from plotly.subplots import make_subplots

    args = marimo.cli_args()
    DB_URL = args["db-url"]

    return DB_URL, graph_objects, make_subplots, marimo, pandas, psycopg2


@app.cell
def clock_cell(marimo):
    refresh = marimo.ui.refresh(default_interval="2s")
    return (refresh,)


@app.cell
def window_cell(marimo):
    window_picker = marimo.ui.dropdown(
        options={"1 min": 60, "5 min": 300, "1 hour": 3600, "1 day": 86400},
        value="1 min",
        label="Window",
    )
    return (window_picker,)


@app.cell
def query_cell(DB_URL, marimo, pandas, psycopg2, refresh, window_picker):
    # re-runs automatically every time `refresh` ticks or window changes
    refresh
    window_seconds = window_picker.value

    # bucket size per window: None = raw data
    BUCKET_SECONDS = {60: None, 300: 1, 3600: 30, 86400: 600}
    bucket = BUCKET_SECONDS.get(window_seconds)

    METRICS_COLS = "temp_inlet, temp_outlet, pressure, flow_rate, voltage, current"

    # compute left border in Python; snap to bucket boundary when bucketing
    query_now = pandas.Timestamp.utcnow()
    _raw_start = query_now - pandas.Timedelta(seconds=window_seconds)
    if bucket is None:
        left_border_dt = _raw_start
    else:
        _origin = pandas.Timestamp("2001-01-01", tz="UTC")
        _bucket_td = pandas.Timedelta(seconds=bucket)
        left_border_dt = _origin + ((_raw_start - _origin) // _bucket_td) * _bucket_td

    params = {"left_border": left_border_dt.to_pydatetime(), "query_now": query_now.to_pydatetime()}

    if bucket is None:
        sql = f"""
            SELECT rig_id, timestamp, {METRICS_COLS}
            FROM   measures
            WHERE  timestamp >= %(left_border)s AND timestamp < %(query_now)s
            ORDER  BY rig_id, timestamp
        """
    else:
        sql = f"""
            SELECT rig_id,
                   date_bin(
                       ('{bucket} seconds')::INTERVAL,
                       timestamp,
                       '2001-01-01'::TIMESTAMP
                   ) AS timestamp,
                   AVG(temp_inlet)  AS temp_inlet,
                   AVG(temp_outlet) AS temp_outlet,
                   AVG(pressure)    AS pressure,
                   AVG(flow_rate)   AS flow_rate,
                   AVG(voltage)     AS voltage,
                   AVG(current)     AS current
            FROM   measures
            WHERE  timestamp >= %(left_border)s AND timestamp < %(query_now)s
            GROUP  BY rig_id, date_bin(
                       ('{bucket} seconds')::INTERVAL,
                       timestamp,
                       '2001-01-01'::TIMESTAMP
                   )
            ORDER  BY rig_id, timestamp
        """

    stats_sql = """
        SELECT
            rig_id,
            ROUND(AVG(temp_inlet)::NUMERIC,  3) AS temp_inlet_mean,
            ROUND(MIN(temp_inlet)::NUMERIC,  3) AS temp_inlet_min,
            ROUND(MAX(temp_inlet)::NUMERIC,  3) AS temp_inlet_max,
            ROUND(AVG(temp_outlet)::NUMERIC, 3) AS temp_outlet_mean,
            ROUND(MIN(temp_outlet)::NUMERIC, 3) AS temp_outlet_min,
            ROUND(MAX(temp_outlet)::NUMERIC, 3) AS temp_outlet_max,
            ROUND(AVG(pressure)::NUMERIC,    3) AS pressure_mean,
            ROUND(MIN(pressure)::NUMERIC,    3) AS pressure_min,
            ROUND(MAX(pressure)::NUMERIC,    3) AS pressure_max,
            ROUND(AVG(flow_rate)::NUMERIC,   3) AS flow_rate_mean,
            ROUND(MIN(flow_rate)::NUMERIC,   3) AS flow_rate_min,
            ROUND(MAX(flow_rate)::NUMERIC,   3) AS flow_rate_max,
            ROUND(AVG(voltage)::NUMERIC,     3) AS voltage_mean,
            ROUND(MIN(voltage)::NUMERIC,     3) AS voltage_min,
            ROUND(MAX(voltage)::NUMERIC,     3) AS voltage_max,
            ROUND(AVG(current)::NUMERIC,     3) AS current_mean,
            ROUND(MIN(current)::NUMERIC,     3) AS current_min,
            ROUND(MAX(current)::NUMERIC,     3) AS current_max
        FROM measures
        WHERE timestamp >= %(left_border)s AND timestamp < %(query_now)s
        GROUP BY rig_id
        ORDER BY rig_id
    """

    try:
        with psycopg2.connect(DB_URL) as _conn:
            df       = pandas.read_sql(sql,       _conn, params=params)
            stats_df = pandas.read_sql(stats_sql, _conn, params=params)
        error_msg = None
    except Exception as exc:
        df       = pandas.DataFrame()
        stats_df = pandas.DataFrame()
        error_msg = str(exc)

    status = (
        marimo.callout(marimo.md(f"**DB error:** {error_msg}"), kind="danger")
        if error_msg
        else marimo.md(
            f"**{len(df)} rows** in the last {window_seconds} s &nbsp;·&nbsp; "
            f"rigs: `{'`, `'.join(sorted(df['rig_id'].unique())) if not df.empty else '—'}`"
        )
    )

    return df, error_msg, left_border_dt, query_now, stats_df, status


@app.cell
def stats_cell(marimo, stats_df):
    if stats_df.empty:
        stats_table = marimo.md("_No data in window._")
    else:
        stats_table = marimo.ui.table(stats_df, selection=None, pagination=False)

    return (stats_table,)


@app.cell
def chart_cell(df, graph_objects, left_border_dt, make_subplots, marimo, query_now, refresh, stats_table, status, window_picker):
    METRICS = [
        ("Temp Inlet (°C)",   ["temp_inlet"]),
        ("Temp Outlet (°C)",  ["temp_outlet"]),
        ("Pressure (bar)",    ["pressure"]),
        ("Flow Rate (L/min)", ["flow_rate"]),
        ("Voltage (V)",       ["voltage"]),
        ("Current (A)",       ["current"]),
    ]
    N_ROWS = len(METRICS)
    V_SPACING = 0.03   # extra room for the per-subplot legends
    subplot_h = (1 - V_SPACING * (N_ROWS - 1)) / N_ROWS

    # bottom edge of each subplot in paper coordinates (top-down)
    subplot_bottoms = [
        1.0 - i * (subplot_h + V_SPACING) - subplot_h
        for i in range(N_ROWS)
    ]

    # one horizontal legend per subplot, sitting just below it
    legends_layout = {}
    for idx, bottom in enumerate(subplot_bottoms):
        key = "legend" if idx == 0 else f"legend{idx + 1}"
        legends_layout[key] = dict(
            orientation="h",
            x=0, y=bottom - 0.01,
            xanchor="left", yanchor="top",
            bgcolor="rgba(0,0,0,0)",
            font=dict(size=10),
        )

    fig = make_subplots(
        rows=N_ROWS, cols=1,
        shared_xaxes=True,
        subplot_titles=[m[0] for m in METRICS],
        vertical_spacing=V_SPACING,
    )
    fig.update_layout(
        height=3200,
        margin=dict(l=60, r=20, t=50, b=40),
        uirevision=N_ROWS,
        **legends_layout,
    )
    fig.update_xaxes(showticklabels=True, title_text="Time (UTC)", range=[left_border_dt, query_now])

    if not df.empty:
        rig_ids = sorted(df["rig_id"].unique())
        all_pairs = [
            (r, m)
            for r in rig_ids
            for _, col_names in METRICS
            for m in col_names
        ]
        pair_color = {
            pair: f"hsl({int(i * 360 / len(all_pairs))}, 70%, 50%)"
            for i, pair in enumerate(all_pairs)
        }

        for row_i, (_, col_names) in enumerate(METRICS, start=1):
            legend_ref = "legend" if row_i == 1 else f"legend{row_i}"
            for metric_chart_cell in col_names:
                for rig_id_chart_cell in rig_ids:
                    sub_chart_cell = df[df["rig_id"] == rig_id_chart_cell][["timestamp", metric_chart_cell]].dropna()
                    fig.add_trace(
                        graph_objects.Scatter(
                            x=sub_chart_cell["timestamp"],
                            y=sub_chart_cell[metric_chart_cell],
                            mode="lines+markers",
                            name=rig_id_chart_cell,
                            legend=legend_ref,
                            line=dict(color=pair_color[(rig_id_chart_cell, metric_chart_cell)], width=1.5),
                            marker=dict(size=3),
                            showlegend=True,
                        ),
                        row=row_i, col=1,
                    )

    marimo.vstack([
        marimo.hstack([refresh, window_picker, status], justify="start", gap=1),
        marimo.ui.plotly(fig),
        marimo.md("### Summary — avg / min / max per rig"),
        stats_table,
    ])
    return


if __name__ == "__main__":
    app.run()
