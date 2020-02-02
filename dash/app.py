import os
import random

import flask
import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
import pandas as pd

from update_metrics import get_updates

BACKGROUND = '#161a28'
PLOT_BACKGROUND = '#161a28'
N_XTICKS = 100
XTICKS_UPDATE_ALLOWANCE = 20

submission = {'table': 'windowed_submissions', 'max_ts': None, 'x': [], 'y': [], 'df': None}
comment = {'table': 'windowed_comments', 'max_ts': None, 'x': [], 'y': [], 'df': None}
active_subreddit = {'table': 'unique_active_subreddits', 'max_ts': None, 'x': [], 'y1': [], 'y2': [], 'df': None}
 
server = flask.Flask(__name__)
app = dash.Dash(
    __name__,
    server=server,
    external_stylesheets=[dbc.themes.DARKLY]
)
app.layout = html.Div([
        dbc.Row([
            html.H2('Reddit Data Streaming')
        ], no_gutters=True),
        dbc.Row([
            dbc.Col([
                dcc.Graph(id='live-submissions'),
                dcc.Interval(id='submissions-interval', interval=1000)
            ], width=6),
            dbc.Col([
                dcc.Graph(id='live-comments'),
                dcc.Interval(id='comments-interval', interval=1000)
            ], width=6)
        ], no_gutters=True),
        dbc.Row([
            dbc.Col([
                dcc.Graph(id='live-active_subreddits'),
                dcc.Interval(id='active_subreddits-interval', interval=1000)
            ], width=12),
        ], no_gutters=True)
], style={
    'padding-top': 50, 'padding-bottom': 50,
    'padding-left': 100, 'padding-right': 100,
    'backgroundColor': BACKGROUND
})

def get_layout(title):
    return {
        'title': {'text': title, 'font': {'size': '20'}},
        'paper_bgcolor': BACKGROUND,
        'plot_bgcolor': PLOT_BACKGROUND,
        'xaxis': {'title': {'text': 'Window'}, 'ticks': 'outside'},
        'yaxis': {'title': {'text': f'No. of {title}'}, 'ticks': 'outside'},
        'margin': {'t': 80, 'b': 80, 'l': 50, 'r': 50},
        'font': {'family': 'Lato', 'color': 'white', 'size': '10'}
    }

def update_state(state, update):
    update = list(zip(*update))
    if state['df'] is None:
        state['df'] = pd.DataFrame(update)
    else:
        state['df'] = state['df'].append(update, ignore_index=True)
        state['df'] = state['df'].groupby(0).max().reset_index()
    state['df'] = state['df'].sort_values(0)[-N_XTICKS:]
    state['x'] = state['df'][0].tolist()
    for i in range(1, len(state['df'].columns)):
        idx = '' if i == 1 else i
        state[f'y{idx}'] = state['df'][i].tolist()
    state['max_ts'] = state['x'][-1]

def template(x, y, count_name='Count', mean_name='Mean'):
    count = {
        'x': x, 'y': y, 
        'type': 'scatter', 
        'name': count_name,
        'line': {'color': '#f9ca24', 'width': '2'}
    }
    mean = {
        'x': x, 'y': [sum(y) / len(y) for _ in range(len(y))], 
        'type': 'scatter', 
        'name': mean_name,
        'line': {'dash': 'dash', 'color': '#22a6b3', 'width': '2'}
    }
    return count, mean

@app.callback(Output('live-submissions', 'figure'),
              [Input('submissions-interval', 'n_intervals')])
def update_submissions(n):
    global submission
    rows = get_updates(XTICKS_UPDATE_ALLOWANCE, **submission)
    if rows:
        _, window, count = list(zip(*rows))
        update_state(submission, [window, count])
    count, mean = template(submission['x'], submission['y'])
    return {'data': [count, mean], 'layout': get_layout('Submissions')}

@app.callback(Output('live-comments', 'figure'),
              [Input('comments-interval', 'n_intervals')])
def update_comments(n):
    global comment
    rows = get_updates(XTICKS_UPDATE_ALLOWANCE, **comment)
    if rows:
        _, window, count = list(zip(*rows))
        update_state(comment, [window, count])
    count, mean = template(comment['x'], comment['y'])
    return {'data': [count, mean], 'layout': get_layout('Comments')}

@app.callback(Output('live-active_subreddits', 'figure'),
              [Input('active_subreddits-interval', 'n_intervals')])
def update_active_subreddits(n):
    global active_subreddit
    rows = get_updates(XTICKS_UPDATE_ALLOWANCE, **active_subreddit)
    if rows:
        _, window, active_subreddits, submissions_and_comments = list(zip(*rows))
        update_state(active_subreddit, [window, active_subreddits, submissions_and_comments])
    count, mean = template(active_subreddit['x'], active_subreddit['y'],
                           'Count(Active Subreddits)', 'Mean(Active Subreddits)')
    count_2 = {
        'x': active_subreddit['x'], 'y': active_subreddit['y2'], 
        'type': 'scatter', 'name': 'Submissions + Comments',
        'line': {'color': '#9b59b6', 'width': '2'}
    }
    return {'data': [count, mean, count_2], 'layout': get_layout('Active Subreddits')}


if __name__ == '__main__':
    DEBUG = os.environ.get('DASH_DEBUG_MODE')
    app.run_server(host='0.0.0.0', port=8050, debug=DEBUG)
