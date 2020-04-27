import pandas as pd
from splinter import Browser
from bs4 import BeautifulSoup
import requests
import pymongo
import pandas as pd
import os
from flask import Flask, render_template, redirect 
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

    df_song.to_csv('./ETL_Project/sample_song_scrapped.csv')


    df_song2=df_song[['song_pop','artist']]
    df_song2.head()

    df_song2['song']=df_song2['artist'].apply(lambda x: x[1])
    df_song2['artist']=df_song2['artist'].apply(lambda x: x[0])

    df_song2.set_index('song_pop', inplace=True)

    df_song2.index
    df_song2.to_csv('./ETL_Project/sample_song_scrappedfinal.csv')

    return df_song2

def main_db_load():
    connection = "mongodb://localhost:27017"
    client = pymongo.MongoClient(connection)
    db= client.etl_db
    table_guardian = db.guardian


    df_guardian=df_song2()
    htlm_table1={'df2': df_guardian.to_html()}
    table_guardian.replace_one({}, htlm_table1, upsert=True)

    # df2.head()
    df2 = pd.read_csv('./ETL_Project/Spotify_2.csv').set_index('id')
    df2.index = df2.index.astype('O')
    df3 = df2.drop(columns=['Beats_Per_Minute','Energy','Danceability','Loudness__dB__',\
                            'Liveness','Valence_','Length_','Acousticness__','Speechiness_','Popularity'])
  
    df3.index = df3.index.astype(str)
    htlm_table={'df3':df3.to_html()}
    table_spotify = db.spotify
    table_spotify.replace_one({}, htlm_table, upsert=True)

    

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
