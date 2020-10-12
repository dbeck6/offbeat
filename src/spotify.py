import tekore as tk
from time import time


class SpotifyInterface:
    '''
    Retrieves song data from recent album releases and featured playlists from 
    the Spotify API. The client id and password are exported as environment 
    variables to bypass the client credentials manager.
    '''

    def __init__(self):
        
        file = 'tekore.cfg'
        conf = tk.config_from_file(file, return_refresh=True)
        token = tk.refresh_user_token(*conf[:2], conf[3])
        self.tk = tk.Spotify(token)

    def get_music(self, num_songs=10, offset=0, playback='recent'):

        if playback == 'recent':
            songs = self.get_recent_playback(num_songs) 
        elif playback == 'playlist':
            songs = self.get_user_playlists(num_tracks=num_songs)
        else:
            return None

        return songs

    def search_track_info(self, artist, track):
        
        tracks, = self.tk.search(f'{track} artist:{artist}', types=('track',))
        try:
            track = tracks.items[0]
            song_id = track.id
            popularity = track.popularity
            url = track.external_urls.get('spotify')
            track_info = {'source_id': song_id, 'popularity': popularity, 'url': url}
        except:
            track_info = None

        return track_info

    def get_recent_playback(self, num_items=20, num_tracks=float('Inf')):
        
        playback = self.tk.playback_recently_played(limit=num_items)
        
        track_data = []
        for i in range(num_items):

            track = playback.items[i].track
            new_track_data = self.process_track(track)
            track_data.append(new_track_data)
                
        return track_data

    def get_user_playlists(self, num_items=20, num_tracks=100, index=0):

        playlist = self.tk.followed_playlists(limit=num_items).items[index]
        playlist = self.tk.playlist_items(playlist.id, limit=num_tracks)
        
        track_data = []
        for i in range(num_tracks):

            track = playlist.items[i].track
            new_track_data = self.process_track(track)
            track_data.append(new_track_data)
        
        return track_data

    def process_track(self, track):

        track_info = self.extract_track_info(track)
        track_feats = self.extract_track_features(track_info.get('source_id'))
        track_data = {**track_info, **track_feats}

        return track_data

    def extract_track_info(self, track):

        track_id = track.id
        track_name = track.name.lower()
        track_year = int(track.album.release_date.split('-')[0])
        artist_name = track.artists[0].name.lower()
        popularity = track.popularity
        url = track.external_urls.get('spotify')
        song_int_id = track_year * track.duration_ms
        track_info = {'source_id': track_id, 'id': song_int_id, 'name': track_name, 'artist': artist_name, 'year': track_year, 'popularity': popularity, 'url': url}
        
        return track_info

    def extract_track_features(self, track_id):

        segments = self.tk.track_audio_analysis(track_id).segments

        timbre = []*len(segments)
        chroma = []*len(segments)
        for segment in segments:

            timbre.append(segment.timbre.asbuiltin())
            chroma.append(segment.pitches.asbuiltin())

        track_features = {'timbre': timbre, 'chroma': chroma}
        
        return track_features


def main():

    si = SpotifyInterface()
    songs = si.get_user_playlists(num_items=10, num_tracks=10)
    print(len(songs))
    print(songs[0].keys())
    print(songs[0].get('name'))
    print(songs[0].get('source_id'))
    print(songs[0].get('id'))
    print(songs[0].get('year'))
    print(songs[0].get('artist'))
    print(songs[0].get('popularity'))
    print(songs[0].get('url'))
    #print(songs[0].get('chroma'))

if (__name__ == '__main__'):
    
    main()
