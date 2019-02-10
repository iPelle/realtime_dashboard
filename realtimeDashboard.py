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

app = dash.Dash('vehicle-data')

def list_of_available_id():
    conn = sqlite3.connect(db_name)
    query = "SELECT name FROM sqlite_master WHERE type='table'"
    c = conn.execute(query)
    tuple_of_tables = list(c)
    list_of_tables = [i[0] for i in tuple_of_tables]
    print(list_of_tables)
    return list_of_tables

list_of_id = list_of_available_id()
print("Type and string of first ID is: {} and {}".format(list_of_id[0], type(list_of_id[0])))
print("Length of first ID is: {}".format(len(list_of_id[0])))
###################################
# App layout
###################################

app.layout = html.Div([
    html.Div([
        html.H2('Vehicle Data',
                style={'float': 'left',
                       }),
        ]),
    dcc.Dropdown(id='machine_id',
                 options=[{'label': s, 'value': s}
                          for s in list_of_id],
                 value=[list_of_id[0]],
                 multi=False
                 ),
    dcc.Dropdown(id='data-name',
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
    dash.dependencies.Output('data-name', 'options'),
    [dash.dependencies.Input('machine_id', 'value')]
)
def update_data_dropdown(machine_id):
    print("EXECUTING UPDATE_DATA_DROPDOWN")
    
    # Added logic here to check if list or string
    if type(machine_id) is list:
        print("Input is a list")
        select_machine_id = machine_id[0]
    else:
        select_machine_id = machine_id
        
    conn = sqlite3.connect(db_name)
    print(type(select_machine_id))
    print("In the callback, the machine value length is: {} and the string is: {}".format(len(select_machine_id),select_machine_id))
    query = 'select * from {} LIMIT 1'.format(select_machine_id)
    print(query)
    c = conn.execute(query)
    names = [description[0] for description in c.description]
    print(names)
    names.remove('id')
    names.remove('datetime')
    conn.close()
    return [{'label': i, 'value': i} for i in names]


@app.callback(
    dash.dependencies.Output('graphs','children'),
    [dash.dependencies.Input('data-name', 'value')
    ,dash.dependencies.Input('machine_id', 'value')
    ],
    events=[dash.dependencies.Event('graph-update', 'interval')]
    )
def update_graph(data_names, machine_id):
    print("EXECUTING UPDATE_GRAPH")
    
    if type(machine_id) is list:
        print("Input is a list")
        machine_id = machine_id[0]
        print()
    else:
        machine_id = machine_id
    
    graphs = []
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    print("Machine_id is: {}".format(machine_id))
    sql_read_data = 'SELECT * FROM {} ORDER BY datetime DESC LIMIT {} '.format(machine_id, plot_limit)
    print(sql_read_data )
    df = pd.read_sql(sql_read_data, conn)
    #print(df)
    #df['datetime']
    #TODO: Update filtering for dropdown of machine-id
    #TODO: Update dropdown-logic, so they depend on eachother (now alternating btween None and value, du to independency)
    
    try:
        if len(data_names)>2:
            class_choice = 'col s12 m6 l4'
        elif len(data_names) == 2:
            class_choice = 'col s12 m6 l6'
        else:
            class_choice = 'col s12'
    except:
        class_choice = 'col s12'
    
    #original
    '''
    if len(data_names)>2:
        class_choice = 'col s12 m6 l4'
    elif len(data_names) == 2:
        class_choice = 'col s12 m6 l6'
    else:
        class_choice = 'col s12'
'''

    try:
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
    except:
        a = 1

    return graphs

    
external_css = ["https://cdnjs.cloudflare.com/ajax/libs/materialize/0.100.2/css/materialize.min.css"]
for css in external_css:
    app.css.append_css({"external_url": css})

external_js = ['https://cdnjs.cloudflare.com/ajax/libs/materialize/0.100.2/js/materialize.min.js']
for js in external_js:
    app.scripts.append_script({'external_url': js})


if __name__ == '__main__':
    app.run_server(debug=True)