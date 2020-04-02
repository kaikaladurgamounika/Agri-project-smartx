import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly
from dash.dependencies import Input, Output
from datetime import datetime, timezone

# Variables
Sched = 24 # Frequency of reporting

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

def get_cursor():
    import pymysql

    # Define Variables
    # Define Variables
    DBHost = 'mysqldb1.cmwln1uaaae0.ap-south-1.rds.amazonaws.com'
    DBUser = 'admin'
    DBPass = 'smartx123'
    DBName = 'waterfall'

    db = pymysql.connect(host=DBHost, user=DBUser, passwd=DBPass, db=DBName)
    crsr = db.cursor()

    return db, crsr



app.layout = html.Div(children=[
    html.H1(children='Smart Agriculture'),

    html.Div(children='''
        Tea Plantation Project - Dryer Parameter Metrics
    '''),

    dcc.Graph(id='live-update-graph'),
    dcc.Interval(
        id='interval-component',
        interval=60*1000, # in milliseconds
        n_intervals=0
    ),
    html.Div(id='live-update-text'),
])


@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_graph_live(n):
    data = {
        'Time': [],
        'Current': [],
        'High': [],
        'Low': []
    }

    db, crsr = get_cursor()
    cmd = '''select * from temp_reading WHERE timestamp > (NOW() - INTERVAL %s HOUR)''' % (Sched,)
    crsr.execute(cmd)
    dd = crsr.fetchall()


    for d in dd:
        dt = d[1].replace(tzinfo=timezone.utc).astimezone(tz=None).strftime("%d/%m/%Y %I:%M%p")
        x = d[1].replace(tzinfo=timezone.utc).astimezone(tz=None).strftime("%I:%M%p")
        ct = d[2]
        mx = d[3]
        mn = d[4]
        data['Time'].append(x)
        #data['Time'] = data['Time'] + [x]
        data['Current'].append(ct)
        data['High'].append(mx)
        data['Low'].append(mn)


    # Create the graph with subplots
    fig = plotly.tools.make_subplots(rows=1, cols=1, vertical_spacing=0.2)
    fig['layout']['margin'] = {
        'l': 30, 'r': 0, 'b': 60, 't': 10
    }
    fig['layout']['legend'] = {'x': 1, 'y': 0.35, 'xanchor': 'auto'}
    fig['layout']['title']: 'Dryer-1 Visualization'

    fig.append_trace({
        'x': data['Time'],
        'y': data['Current'],
        'name': 'Temperature',
        'mode': 'lines+markers',
        'type': 'scatter'
    }, 1, 1)
    fig.append_trace({
        'x': data['Time'],
        'y': data['High'],
        'text': data['Time'],
        'name': 'Higher Limit',
        'mode': 'lines+markers',
        'type': 'scatter'
    }, 1, 1)
    fig.append_trace({
        'x': data['Time'],
        'y': data['Low'],
        'text': data['Time'],
        'name': 'Lower Limit',
        'mode': 'lines+markers',
        'type': 'scatter'
    }, 1, 1)
    return fig


@app.callback(Output('live-update-text', 'children'),
              [Input('interval-component', 'n_intervals')])
def update_metrics(n):
    db, crsr = get_cursor()
    cmd = '''select * from alerts group by `timestamp` desc'''
    crsr.execute(cmd)
    alert = crsr.fetchall()

    alerts = ""
    for a in alert:
        if alerts:
            alerts = alerts + " \n"
        else:
            alerts = alerts
        dt = a[0]
        status = a[1]
        start = a[2]
        end = a[3]
        duration = a[4]
        peak = a[5]

        if status == 'high':
            alerts = alerts + '''%s : Temperature exceeded maximum limit from %s to %s for %s, and peak value was %s''' % (
            str(dt), str(start), str(end), str(duration), str(peak))
        elif status == 'low':
            alerts = alerts + '''%s : Temperature  is less than minimum limit from %s to %s for %s, and peak value was %s''' % (
            str(dt), str(start), str(end), str(duration), str(peak))

    cmd = '''select * from toggle'''
    crsr.execute(cmd)
    check = crsr.fetchall()

    if check:
        status = check[0][0]
        start = check[0][1]
        current = check[0][3]
        stamp = datetime.now().strftime("%d/%m/%Y %I:%M%p")
        if status == 'high':
            a = '''%s : Temperature exceeded maximum limit at %s and current value is %s degrees''' % (
            str(stamp), str(start), str(current))
            alerts = a + "\n" + alerts
        if status == 'low':
            a = '''%s : Temperature is less than minimum limit at %s and current value is %s degrees''' % (
            str(stamp), str(start), str(current))
            alerts = a + "\n" + alerts

    print(alerts)
    return [
        html.H2(children='Alerts'),
        html.Plaintext(alerts)
    ]


if __name__ == '__main__':
    app.run_server(debug=True,host='0.0.0.0')
