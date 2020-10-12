# Offbeat: Find great songs you didn't know existed 
Insight Data Science Project - Fall 2020

Offbeat is a fully functional data pipeline and web app that utilizes feature similarity search and popularity metrics to generate user-curated music recommendations. A Spark cluster is used to compute in parallel an embedding vector from the timbre and chroma (pitches) features of songs from the Million Song Dataset and Spotify. The resulting vectors are written to both a Hierarchical Navigable Small World Graph (HNSW) based index constructed by Hnswlib and a PostgresSQL database alongside the song's metadata. The Dash-developed web app returns the results of the similarity search for a song which can be filtered by popularity scores and pushed to a new Spotify playlist created by the app on the user's account.

## Tech Stack
![Image of tech stack](https://raw.githubusercontent.com/dbeck6/offbeat/master/imgs/Screenshot%202020-10-12%20164557.png)

## Data and Dependencies
- The Million Song Dataset is loaded from **AWS EBS** from the [public snapshot](https://aws.amazon.com/datasets/million-song-dataset/).
- The **Spotify Web API** is accessed through the **Tekore** library to retrieve user's most recently listened songs and to create playlists.
- An independently created **Spark** cluster is used to calculate the embedding vectors in parallel for songs from both sources.
- **Hnswlib** is used to create an HNSW index for song similarity search.
- A **PostgresSQL** database stores the song metadata and embedding vectors.
- **Airflow** automatically retrieves user's recently played songs for processing and updating the database.
- **Dash** renders the web app for displaying the similarity search results and generates a playlist for the user based on the selected scope of popularity scores.

[*Demo link*](http://youtu.be/GiHtN6Y6FY4?hd=1)

### Sources
1. [Million Song Dataset](http://millionsongdataset.com/)
2. [Tekore, a Python client for Spotify Web API](https://github.com/felix-hilden/tekore)
3. [Hnswlib, Header-only C++/python library for fast approximate nearest neighbors](https://github.com/nmslib/hnswlib)
