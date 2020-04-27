import pandas as pd
from splinter import Browser
from bs4 import BeautifulSoup
import requests
import pymongo
import pandas as pd
import os
from flask import Flask, render_template, redirect
# Create an instance of Flask
app = Flask(__name__)
def df_song2():
    url='https://www.theguardian.com/music/2019/dec/02/the-20-best-songs-of-2019-tracks'
    
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html.parser')

    
    # Examine the results, then determine element that contains sought info
    

    section = soup.find('div', class_='content__article-body from-content-api js-article__body')

    song_data=section.find_all('h2')

    i=1

    song_dic={
    'song_pop':[],
    'song_name':[]
    }

    for song in song_data:
        if (i % 2 == 0):
            song_dic['song_pop'].append(song_data[i-2].text)
            song_dic['song_name'].append(song_data[i-1].text)
            print(song_data[i-2].text,'---->',song_data[i-1].text)
            i+=1
        else:
            i+=1
        print(song_dic)

    df_song=pd.DataFrame(song_dic)
    df_song['song_name'].str.split('–')[0][0]
    df_song['artist']=df_song['song_name'].str.split('–')
    df_song.head()

    df_song2=df_song[['song_pop','artist']]
    df_song2.head()

    df_song2['song']=df_song2['artist'].apply(lambda x: x[1])
    df_song2['artist']=df_song2['artist'].apply(lambda x: x[0])

    # df_song2.set_index('song_pop', inplace=True)

    df_song2.index
    df_song2.to_csv('C:/Users/User/ETL Combine/Scrappedfinal.csv')

    return df_song2

def main_db_load():
    #mongo = PyMongo(app, uri="mongodb://localhost:27017/mars_app")
    connection = "mongodb://localhost:27017"
    client = pymongo.MongoClient(connection)
    db= client.etl_db
    table_guardian = db.guardian
    
    

    df_guardian=df_song2()
    df_guardian.columns = ['Track Number', 'Artist', 'Song']
    # df_guardian.rename_axis('Track Number')
    df_format1=df_guardian.to_html(index=False).replace('<table border="1" class="dataframe">','<table class="table table-striped">')
    df_format1=df_format1.replace('<tr style="text-align: right;">','<tr style="text-align: left;">')
    html_table1={'df2': df_format1}
    table_guardian.replace_one({}, html_table1, upsert=True)

    # df2.head()
    df2 = pd.read_csv('C:/Users/User/ETL Combine/Spotify_2.csv').set_index('id')
    df2.index = df2.index.astype('O')
    df3 = df2.drop(columns=['Beats_Per_Minute','Energy','Danceability','Loudness__dB__',\
                            'Liveness','Valence_','Length_','Acousticness__','Speechiness_','Popularity'])
  
    df3.index = df3.index.astype(str)

    df_format2=df3.to_html(index=False).replace('<table border="1" class="dataframe">','<table class="table table-striped">')
    df_format2=df_format2.replace('<tr style="text-align: right;">','<tr style="text-align: left;">')
    html_table2={'df3':df_format2}
    table_spotify = db.spotify
    table_spotify.replace_one({}, html_table2, upsert=True)

        

    guardian_table_dic=table_guardian.find_one({})
    spotify_table_dic=table_spotify.find_one({})
   

    return guardian_table_dic,spotify_table_dic
@app.route("/")
def index():
    g_data, s_data = main_db_load()
    return render_template("index.html", gdata=g_data, sdata = s_data)


if __name__ == "__main__":
    
    x,y=main_db_load()
    print(x)
    app.run(debug=True)
