# plot messages from pub sub
import dash
from dash.dependencies import Output, Event
import dash_core_components as dcc
import dash_html_components as html
import plotly
import pandas as pd
import time
import plotly.graph_objs as go

import sqlite3

import multiprocessing
import base64


table_name = "streaming_data"
db_name = "streaming_db"

data_dict = {}
new_dict= {}
first_data_received = 0
counter = 0
plot_limit = 100

column_names = None

app = dash.Dash('vehicle-data')


conn = sqlite3.connect(db_name)
c = conn.cursor()
list_of_column_names =  "SELECT * FROM {} LIMIT {}".format(table_name, plot_limit)


def list_of_table_column_name():
    conn = sqlite3.connect(db_name)
    c = conn.execute('select * from streaming_data LIMIT 1')
    names = [description[0] for description in c.description]
    conn.close()
    return names
    
column_names = list_of_table_column_name()

###################################
# App layout
###################################

app.layout = html.Div([
    html.Div([
        html.H2('Vehicle Data',
                style={'float': 'left',
                       }),
        ]),
    dcc.Dropdown(id='vehicle-data-name',
                 options=[{'label': s, 'value': s}
                          for s in column_names],
                 value=[column_names[1]],
                 multi=True
                 ),
    html.Div(children=html.Div(id='graphs'), className='row'),
    dcc.Interval(
        id='graph-update',
        interval=1*1000),
    ], className="container",style={'width':'98%','margin-left':10,'margin-right':10,'max-width':50000})

###################################
# Render plots
###################################

@app.callback(
    dash.dependencies.Output('graphs','children'),
    [dash.dependencies.Input('vehicle-data-name', 'value')],
    events=[dash.dependencies.Event('graph-update', 'interval')]
    )
def update_graph(data_names):
    graphs = []
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    sql_read_data = "SELECT * FROM {} ORDER BY datetime DESC LIMIT {}".format(table_name, plot_limit)
    df = pd.read_sql(sql_read_data, conn)
    #df['datetime']


    if len(data_names)>2:
        class_choice = 'col s12 m6 l4'
    elif len(data_names) == 2:
        class_choice = 'col s12 m6 l6'
    else:
        class_choice = 'col s12'


    for data_name in data_names:

        data = go.Scatter(
            x=list(df['datetime']),
            y=list(df[data_name]),
            name='Scatter',
            fill="tozeroy",
            fillcolor="#6897bb"
            )

        graphs.append(html.Div(dcc.Graph(
            id=data_name,
            animate=True,
            figure={'data': [data],'layout' : go.Layout(xaxis=dict(range=[min(df['datetime']),max(df['datetime'])]),
                                                        yaxis=dict(range=[min(df[data_name]),max(df[data_name])]),
                                                        margin={'l':50,'r':1,'t':45,'b':1},
                                                        title='{}'.format(data_name))}
            ), className=class_choice))

    return graphs

    
external_css = ["https://cdnjs.cloudflare.com/ajax/libs/materialize/0.100.2/css/materialize.min.css"]
for css in external_css:
    app.css.append_css({"external_url": css})

external_js = ['https://cdnjs.cloudflare.com/ajax/libs/materialize/0.100.2/js/materialize.min.js']
for js in external_js:
    app.scripts.append_script({'external_url': js})


if __name__ == '__main__':
    app.run_server(debug=True)