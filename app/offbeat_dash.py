#!/usr/bin/env python3
import os
import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import sqlalchemy
import pandas as pd
import numpy as np
import hnswlib
import tekore as tk

def connect():

    user = os.environ['POSTGRES_UN']
    password = os.environ['POSTGRES_PW']
    host = os.environ['POSTGRES_HOST']
    db = 'songs_db'
    port = 5432
    url = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(user, password, host, port, db)
    conn = sqlalchemy.create_engine(url, client_encoding='utf8')

    return conn

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

project_path = '/home/ubuntu/offbeat/src/'
index_filename = 'index.bin'
# initiate hnswlib index
num_elements = 2000000
dim = 180
index = hnswlib.Index(space = 'cosine', dim = dim)
index.load_index(os.path.join(project_path, index_filename), max_elements = num_elements)
index.set_ef(50)
# initiate spotify
token_file = 'tekore.cfg'
conf = tk.config_from_file(os.path.join(project_path, token_file), return_refresh=True)
token = tk.refresh_user_token(*conf[:2], conf[3])
spotify = tk.Spotify(token)
conn = connect()

num_neighbors = 20
num_initial_rows = 100
initial_query = "SELECT si.name, si.artist, si.popularity, si.url FROM song_info as si LIMIT {}".format(num_initial_rows)
default_df = pd.read_sql_query(initial_query, conn)
default_df['rank'] = np.arange(1, num_initial_rows + 1)
default_df['distance'] = np.zeros(num_initial_rows)
default_df = default_df[['rank', 'distance', 'name', 'artist', 'popularity', 'url']]

colors = {
    'background': '#111111',
    'text': '#D63BEE'
}

app.layout = html.Div([

    html.Div(children=html.H5(children='Offbeat: Find great songs you didn’t know existed', style={'textAlign': 'center', 'color': colors['text']})),
    dcc.Input(
        id='song_name',
        type='text',
        placeholder='Enter a song name'
    ),
    dcc.Input(
        id='artist_name',
        type='text',
        placeholder='Enter the artist for song'
    ),
    html.Button(id='submit_button', type='submit', children='Submit'),
    html.Label('Make playlist?'),
    dcc.Input(
        id='playlist_name',
        type='text',
        placeholder='Enter a name for playlist'
    ),
    dcc.RadioItems(
        id='playlist_create',
        options=[
            {'label': 'Yes', 'value': 'Yes'},
            {'label': 'No', 'value': 'No'}
        ],
        value='No',
        labelStyle={'display': 'inline-block'}
    ),
    html.Div(children=html.H5(children='Popularity Scale')),
    dcc.RangeSlider(
        id='range_slider',
        min=0,
        max=100,
        step=1,
        marks={i: str(i) for i in range(0, 101, 5)},
        value=[0, 100]
    ),
    html.Div(children=html.H5(children='Most Similar Songs')),
    dash_table.DataTable(
        id='table',
        css=[{
            'selector': '.dash-cell div.dash-cell-value',
            'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
        }],
        columns=[{'name': i, 'id': i} for i in default_df.columns]
    )
    
])

@app.callback(
    Output(component_id='table', component_property='data'),
    [Input(component_id='submit_button', component_property='n_clicks')],
    [State(component_id='song_name', component_property='value'),
    State(component_id='artist_name', component_property='value'),
    State(component_id='playlist_name', component_property='value'),
    State(component_id='playlist_create', component_property='value'),
    State(component_id='range_slider', component_property='value')]
)
def update_table(n_clicks, song_name, artist_name, playlist_name, playlist_create, range_slider):
    # The button was clicked without entering a song name
    if (not song_name and not artist_name):
        return default_df.loc[(default_df['popularity'] >= range_slider[0]) & (default_df['popularity'] <= range_slider[1])].to_dict('records')

    song_df, song_list = get_recommendations(song_name, artist_name, range_slider)

    if playlist_create == 'Yes' and playlist_name != '':
        uris = [spotify.track(t).uri for t in song_list]
        user = spotify.current_user()
        playlist = spotify.playlist_create(
            user.id,
            playlist_name,
            public=False,
            description=f"Offbeat recommendations for {song_name} by {artist_name}"
        )
        spotify.playlist_add(playlist.id, uris=uris)
    
    return song_df.to_dict('records')

def get_recommendations(song_name, artist_name, range_slider):
    # Get the vector for the given song name
    song_vector_query = "SELECT sv.vector \
                        FROM song_info as si INNER JOIN song_vectors as sv \
                        ON si.id = sv.id \
                        WHERE si.name='{song_name}' AND si.artist='{artist_name}'"
    vector_query = song_vector_query.format(song_name=song_name,artist_name=artist_name)
    vector_df = pd.read_sql_query(vector_query, conn)
    
    # The song name can't be found in the DB
    if (vector_df.empty):
        return default_df.loc[(default_df['popularity'] >= range_slider[0]) & (default_df['popularity'] <= range_slider[1])], []
    
    # Run the similarity search on the vector
    vector = vector_df['vector'].values[0]
    vector = np.array(vector, dtype=np.float32)
    vector = np.expand_dims(vector, axis=0)
    ids, dists = index.knn_query(vector, num_neighbors + 1)
    dists = dists[0, :]
    ids = ids[0, :]
    
    # Get the song info of the most similar songs
    song_info_query = "SELECT si.source_id, si.id, si.name, si.artist, si.popularity, si.url \
                    FROM song_info as si \
                    WHERE si.id in "
    ids_format = "{}," * (num_neighbors + 1)
    song_info_query += '(' + ids_format[:-1] + ')'
    song_query = song_info_query.format(*ids)
    song_df = pd.read_sql_query(song_query, conn)
    song_df = song_df.loc[(song_df['popularity'] >= range_slider[0]) & (song_df['popularity'] <= range_slider[1])]
    song_list = song_df['source_id'].tolist()

    # Add distances and ranks
    num_results = len(song_df)
    song_df['rank'] = 0
    song_df['distance'] = 0.0
    for rank, (id, dist) in enumerate(zip(ids, dists)):
        song_df.loc[song_df['id'] == id, 'rank'] = rank
        song_df.loc[song_df['id'] == id, 'distance'] = dist
    song_df.sort_values(by=['rank'], inplace=True)
    song_df = song_df[['rank', 'distance', 'name', 'artist', 'popularity', 'url']]

    return song_df, song_list

if __name__ == "__main__":
    app.run_server(debug=True, host='0.0.0.0', port='5000')
