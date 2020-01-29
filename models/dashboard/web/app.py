# -*- coding: utf-8 -*-
import dash
import dash_html_components as html
import dash_core_components as dcc
import numpy as np
from plotly import graph_objs as go
from dash.dependencies import Input, Output, State
import time
import boto3
import os
import json
import decimal
import random

TABLE_NAME = os.getenv("TABLE_NAME", "cdk-DashboardModel0F8D7DBF-3NKS2U5YZ10Z")

ddb_res = boto3.resource("dynamodb")
table = ddb_res.Table(TABLE_NAME)


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


app = dash.Dash(__name__)

REGIONS = {
    "NORTH": ["AM", "RR", "PA", "TO", "AC", "RO", "MA", "AP"],
    "NORTHEAST": ["MA", "BA", "AL", "PE", "RN", "CE", "PB", "SE", "PI"],
    "SOUTH": ["RS", "PR", "SC"],
    "SOUTHEAST": ["MG", "SP", "RJ", "ES"],
    "MIDWEST": [ "MT", "GO", "MS"]
}

#TIME_SPAN = [ ""]

app.layout = html.Div(
    children=[
        html.H3("Summary"),
        html.Div(
                    [
                        html.Div(
                            [
                                html.Div(
                                    [html.H6(id="kpi1_sum_text"), html.P("KPI 1")],
                                    id="kpi1_sum",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="kpi2_sum_text"), html.P("KPI 2")],
                                    id="kpi2_sum",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="kpi3_sum_text"), html.P("KPI 3")],
                                    id="kpi3_sum",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="kpi4_sum_text"), html.P("KPI 4")],
                                    id="kpi4_sum",
                                    className="mini_container",
                                ),
                            ],
                            id="info-container",
                            className="row container-display",
                        )
                    ]),
        html.H3("Sample KPI Dashboard"),
                html.Div([
            dcc.Dropdown(
                id='crossfilter-xaxis-region',
                options=[{'label': i, 'value': i} for i in REGIONS.keys()],
                value='SOUTHEAST'
            ),
            dcc.Dropdown(
                id='crossfilter-xaxis-state',
                #options=[{'label': i, 'value': i} for i in REGIONS["SOUTHEAST"]],
                value='State'
            ),
            dcc.RadioItems(
                id='crossfilter-xaxis-time',
                options=[{'label': i, 'value': i} for i in ['Minute', '10-Minute', 'Hours']],
                value='Minute',
                labelStyle={'display': 'inline-block'}
            )
        ],
        style={'width': '49%', 'display': 'inline-block'}),
        dcc.Input(id="input-1", value='Input triggers local spinner'),
        dcc.Loading(id="loading-1", children=[html.Div(id="loading-output-1")], type="default"),
        html.Div(
            [
                dcc.Input(id="input-2", value='Input triggers nested spinner'),
                dcc.Loading(
                    id="loading-2",
                    children=[html.Div([html.Div(id="loading-output-2")])],
                    type="circle",
                )
            ]
        ),
         html.Div(
                id="kpi_1_container",
                className="chart_div pretty_container",
                children=[
                    html.P("KPI 1"),
                    dcc.Graph(id="kpi_1", config=dict(displayModeBar=False)),
                ],
            ),
          html.Div(
                id="kpi_2_container",
                className="chart_div pretty_container",
                children=[
                    html.P("KPI 2"),
                    dcc.Graph(id="kpi_2", config=dict(displayModeBar=False)),
                ],
            ),
                html.Div([
        dcc.Graph(
            id='crossfilter-indicator-scatter',
            hoverData={'points': [{'customdata': 'Japan'}]}
        )
    ], style={'width': '49%', 'display': 'inline-block', 'padding': '0 20'}),
    html.Div([
        dcc.Graph(id='x-time-series'),
        dcc.Graph(id='y-time-series'),
    ], style={'display': 'inline-block', 'width': '49%'}),

    html.Div(dcc.Slider(
        id='crossfilter-year--slider',
        #min=df['Year'].min(),
        #max=df['Year'].max(),
        #value=df['Year'].max(),
        #marks={str(year): str(year) for year in df['Year'].unique()},
        step=None
    ), style={'width': '49%', 'padding': '0px 20px 20px 20px'})
    ],
)

def get_pie_chart():
    
    types = ["kpi1", "kpi2", "kpi3", "kpi4"]
    values = [10, 20, 30, 40]

    layout = go.Layout(
        autosize=True,
        margin=dict(l=0, r=0, b=0, t=4, pad=8),
        paper_bgcolor="white",
        plot_bgcolor="white",
    )

    trace = go.Pie(
        labels=types,
        values=values,
        marker={"colors": ["#264e86", "#0074e4", "#74dbef", "#eff0f4"]},
    )

    return {"data": [trace], "layout": layout}


def query_data(region, state, time="PT10M"):
    print("region = {} / state = {} / time = {}".format(region, state, time))

    kpi_x = []
    kpis_y = {
        "kpi1": [],
        "kpi2": [],
        "kpi3": [],
        "kpi4": [],
    }

    pk = region

    if state:
        pk = "{}#{}".format(region, state)

    q = table.query(
        KeyConditionExpression="Pk = :pk AND begins_with(Sk, :ts)",
        ExpressionAttributeValues={
            ":pk": "{}".format(pk),
            ":ts": "{}#".format(time)
        },
        Limit=200,
        ConsistentRead=False,
        ScanIndexForward=False,
        ReturnConsumedCapacity="TOTAL"
    )

    print(q["ConsumedCapacity"])

    if "Items" in q:
        cnt = 0
        for item in q["Items"]:
            
            kpis_y["kpi1"].append(item["kpi1"])
            kpis_y["kpi2"].append(item["kpi2"])
            kpis_y["kpi3"].append(item["kpi3"])
            kpis_y["kpi4"].append(item["kpi4"])

            #kpi1_y.append(item["IngestTime"])
            #kpi1_y.append(random.random())
            #kpi1_y.append(random.randint(0,100))
            kpi_x.append(item["IngestTime"])

            # if cnt % 50 == 0:
            #     print(item)
            cnt += 1
    else:
        print("No data found!")

    print("Records {}".format(cnt))

    return kpi_x, kpis_y


def get_line_chart(region, state=None, store=None, time="Minute"):
    print("get_line_chart time={}".format(time))
    
    kpi1_x = []
    kpis_y = []
    
    span = "PT1M"

    if time == "Hours":
        span = "PT1H"
    elif time == "10-Minute":
        span = "PT10M"

    kpi1_x, kpis_y = query_data(region, state, span)
    
    #print(json.dumps(values, cls=DecimalEncoder))
    
    kpi1 = {'x': kpi1_x, 'y': kpis_y["kpi1"], 'type': 'line', 'name': '{} - KPI1'.format(region)}
    kpi2 = {'x': kpi1_x, 'y': kpis_y["kpi2"], 'type': 'line', 'name': '{} - KPI2'.format(region)}
    kpi3 = {'x': kpi1_x, 'y': kpis_y["kpi3"], 'type': 'line', 'name': '{} - KPI3'.format(region)}
    kpi4 = {'x': kpi1_x, 'y': kpis_y["kpi4"], 'type': 'line', 'name': '{} - KPI4'.format(region)}

    d = {
        "data": [
            kpi1,
            kpi2,
            kpi3,
            kpi4
        ],
        "layout": {
            "title": "Sample dashboard - {}".format(region)
        }
    }

    kpi1_sum = 0

    for k in kpis_y["kpi1"]:
        kpi1_sum += k

    kpi2_sum = 0

    for k in kpis_y["kpi2"]:
        kpi2_sum += k

    kpi3_sum = 0

    for k in kpis_y["kpi3"]:
        kpi3_sum += k

    kpi4_sum = 0

    for k in kpis_y["kpi4"]:
        kpi4_sum += k

    return d, kpi1_sum, kpi2_sum, kpi3_sum, kpi4_sum


@app.callback(
    [Output("kpi_1", "figure"), 
    Output("kpi1_sum_text", "children"),
    Output("kpi2_sum_text", "children"),
    Output("kpi3_sum_text", "children"),
    Output("kpi4_sum_text", "children")], 
    [Input("crossfilter-xaxis-region", "value"), 
    Input("crossfilter-xaxis-state", "value"),
    Input("crossfilter-xaxis-time", "value")])
def input_triggers_spinner_kpi1(value, value_state, time):

    chart, kpi1_sum, kpi2_sum, kpi3_sum, kpi4_sum = get_line_chart(value, value_state, time=time)
    
    return chart, '{:,}'.format(kpi1_sum), '{:,}'.format(kpi2_sum), '{:,}'.format(kpi3_sum), '{:,}'.format(kpi4_sum)


@app.callback(
    Output('crossfilter-xaxis-state', 'options'),
    [Input('crossfilter-xaxis-region', 'value')])
def set_cities_options(selected_region):
    print(selected_region)
    return [{'label': i, 'value': i} for i in REGIONS[selected_region]]

#@app.callback(Output("kpi_2", "figure"), [Input("crossfilter-xaxis-region", "value"), Input("crossfilter-xaxis-state", "value")])
#def input_triggers_spinner_kpi2(value, value_state):
#    return get_line_chart(value, value_state)

# Selectors -> well text
# @app.callback(
#     Output("well_text", "children"),
#     [
#         Input("crossfilter-xaxis-region", "value")
#     ],
# )
# def update_well_text(well_statuses): #, well_types, year_slider):

#     #dff = filter_dataframe(df, well_statuses, well_types, year_slider)
#     return "10"


def filter_dataframe(df, well_statuses, well_types, year_slider):
    dff = df[
        df["Well_Status"].isin(well_statuses)
        & df["Well_Type"].isin(well_types)
        & (df["Date_Well_Completed"] > dt.datetime(year_slider[0], 1, 1))
        & (df["Date_Well_Completed"] < dt.datetime(year_slider[1], 1, 1))
    ]
    return dff


if __name__ == "__main__":
    app.run_server(debug=True)