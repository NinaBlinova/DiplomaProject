from flask import Flask
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go


server = Flask(__name__)

app = Dash(
    __name__,
    server=server,
    url_base_pathname='/dashboard/'
)

app.layout = html.Div([
    html.H4('Interactive color selection with simple Dash example'),
    html.P("Select color:"),
    dcc.Dropdown(
        id="getting-started-x-dropdown",
        options=['Gold', 'MediumTurquoise', 'LightGreen'],
        value='Gold',
        clearable=False,
    ),
    dcc.Graph(id="getting-started-x-graph"),
])


@app.callback(
    Output("getting-started-x-graph", "figure"),
    Input("getting-started-x-dropdown", "value")
)
def update_graph(color):
    fig = go.Figure(
        data=go.Bar(
            x=["A", "B", "C"],
            y=[4, 1, 2],
            marker_color=color
        )
    )
    return fig


@server.route('/')
def hello_world():
    return 'Hello World!'


if __name__ == '__main__':
    server.run(debug=True)
