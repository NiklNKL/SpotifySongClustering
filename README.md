# SpotifySongClustering
This is a student project for the Clustering &amp; Classification module from the FHDW Datascience Master

To run the recommendation engine, please create credentials for the spotify api and save them in a .env file.
This file should look like this at the end:

SPOTIFY_CLIENT_ID=client_id\
SPOTIFY_CLIENT_SECRET=client_secret\
SPOTIFY_REDIRECT_URI=app_uri

Follow the instructions on spotify development page for more information:
https://developer.spotify.com/documentation/web-api

Also make sure you install all libraries in the requirement.txt.\
It is recommended to use a python virtual environment for that.

The use of the recommendation engine is described inside of the song_recommendation notebook.