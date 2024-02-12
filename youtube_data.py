from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
from datetime import timedelta
from datetime import datetime
import streamlit as st
from streamlit_option_menu import option_menu

def api_collect():

    api_key="AIzaSyCfneazn8_upXCKvASaWrHY518s5JaNCoY"
    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name,api_version,developerKey=api_key)

    return youtube
youtube = api_collect()


def get_channel_details(channel_id):
        request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_id)
        response = request.execute()

        for i in response['items']:
                data = dict(Channel_Name = i['snippet']['title'],
                            Channel_Id = i['id'],
                            Subscription_Count =i['statistics']['subscriberCount'],
                            Channel_Views = i['statistics']['viewCount'],
                            Total_Videos = i['statistics']['videoCount'],
                            Channel_Description = i['snippet']['description'],
                            Playlist_Id = i['contentDetails']['relatedPlaylists']['uploads'] )
        return data

def get_video_idS(channel_id):
     Video_Ids = []
     request = youtube.channels().list(
                    part="contentDetails",
                    id=channel_id).execute()
     Playlist_Id = request['items'][0]['contentDetails']['relatedPlaylists']['uploads']
     Next_Page_Token = None
     while True:
          response1 = youtube.playlistItems().list(
                                   part = "snippet",
                                   playlistId = Playlist_Id,
                                   maxResults = 50,
                                   pageToken = Next_Page_Token).execute()
          for i in range(len(response1['items'])):
               Video_Ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
               Next_Page_Token = response1.get('nextPageToken')  

          if Next_Page_Token is None:
               break 
     return  Video_Ids

def convert_duration(duration_str):
    duration_str = duration_str.replace('PT', '')
    
    hours, minutes, seconds = 0, 0, 0
    
    if 'H' in duration_str:
        hours_index = duration_str.index('H')
        hours = int(duration_str[:hours_index])
        duration_str = duration_str[hours_index+1:]
    
    if 'M' in duration_str:
        minutes_index = duration_str.index('M')
        minutes = int(duration_str[:minutes_index])
        duration_str = duration_str[minutes_index+1:]
    
    if 'S' in duration_str:
        seconds_index = duration_str.index('S')
        seconds = int(duration_str[:seconds_index])
    
    duration = timedelta(hours=hours, minutes=minutes, seconds=seconds)
    return str(duration)

def convert_to_datetime(published_date):
    dt = datetime.fromisoformat(published_date.replace('Z', '+00:00'))
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def get_video_details(video_ids):
    Video_Data=[]
    for video_id in video_ids:
        
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id= video_id)
        response2 = request.execute()

        for item in response2['items']:
            data1 = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Video_Name = item['snippet']['title'],
                        Video_Description = item['snippet'].get('description'),
                        Tags ="".join(item['snippet'].get('tags',["NA"])),
                        Published_Date = convert_to_datetime(item['snippet']['publishedAt']),
                        View_Count = item['statistics'].get('viewCount'),
                        Like_Count = item['statistics'].get('likeCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Comment_Count = item['statistics'].get('commentCount'),
                        Duration = convert_duration(item['contentDetails']['duration']),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Caption_Status = item['contentDetails']['caption'])
            Video_Data.append(data1)
    return Video_Data

def get_comment_details(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                            part = "snippet",
                            videoId =video_id,
                            maxResults = 50)
            response = request.execute()

            for item in response['items']:
                data3 =dict(Video_Id =item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Id = item['snippet']['topLevelComment']['id'],
                            Comment_Text =item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author =item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published_Date =convert_to_datetime(item['snippet']['topLevelComment']['snippet']['publishedAt']))
                
                Comment_data.append(data3)

    except:
        pass

    return Comment_data 

client = pymongo.MongoClient("mongodb://localhost:27017")
db=client['youtube_data']

def channel_details(channel_id):
    channel_info = get_channel_details(channel_id)
    video_ids =get_video_idS(channel_id)
    video_info =get_video_details(video_ids)
    comment_info=get_comment_details(video_ids)

    collection1 = db["channel_details"]
    collection1.insert_one({'channel_information': channel_info,'video_information':video_info,
                            'comment_information':comment_info})
    
    return 'success'

def channel_table():
    mydb = mysql.connector.connect( host="localhost",user="root",password="sk23",database='youtube_data')
    cursor = mydb.cursor(buffered=True)
    
    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists channels(Channel_Name varchar(50),
                                                        Channel_Id varchar(100) primary key,
                                                        Subscription_Count bigint,
                                                        Channel_Views bigint,
                                                        Total_Videos bigint,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(100))'''
    cursor.execute(create_query)
    mydb.commit()

    ch_list =[]
    db=client['youtube_data']
    collection1 = db["channel_details"]
    for ch_data in collection1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])     
    df = pd.DataFrame(ch_list)  

    for index,row in df.iterrows():
        insert_query = '''insert into channels(Channel_Name,
                                                Channel_Id,
                                                Subscription_Count,
                                                Channel_Views,
                                                Total_Videos,
                                                Channel_Description,
                                                Playlist_Id)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        
        values=(row['Channel_Name'],
            row['Channel_Id'],
            row['Subscription_Count'],
            row['Channel_Views'],
            row['Total_Videos'],
            row['Channel_Description'],
            row['Playlist_Id'])
        
        
        cursor.execute(insert_query,values)
        mydb.commit()

def video_table(): 
    mydb = mysql.connector.connect( host="localhost",user="root",password="sk23",database='youtube_data')
    cursor = mydb.cursor(buffered=True)

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(80) primary key,
                                                    Video_Name varchar(150),
                                                    Video_Description text,
                                                    Tags text,
                                                    Published_Date datetime,
                                                    View_Count bigint,
                                                    Like_Count bigint,
                                                    Favorite_Count int,
                                                    Comment_Count int,
                                                    Duration time,
                                                    Thumbnail varchar(200),
                                                    Caption_Status varchar(50)
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()

    vi_list =[]
    db=client['youtube_data']
    collection1 = db["channel_details"]
    for vi_data in collection1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])     
    df1 =pd.DataFrame(vi_list)  

    for index,row in df1.iterrows():
            insert_query = '''insert into videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Video_Name,
                                                    Video_Description,
                                                    Tags,
                                                    Published_Date,
                                                    View_Count,
                                                    Like_Count,
                                                    Favorite_Count,
                                                    Comment_Count,
                                                    Duration,
                                                    Thumbnail,
                                                    Caption_Status)
                                            
                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
        
            values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Video_Name'],
                row['Video_Description'],
                row['Tags'],
                row['Published_Date'],
                row['View_Count'],
                row['Like_Count'],
                row['Favorite_Count'],
                row['Comment_Count'],
                row['Duration'],
                row['Thumbnail'],
                row['Caption_Status'])

            cursor.execute(insert_query,values)
            mydb.commit()

def comment_table(): 
    mydb = mysql.connector.connect( host="localhost",user="root",password="sk23",database='youtube_data')
    cursor = mydb.cursor(buffered=True)

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists comments(Video_Id varchar(50),
                                                        Comment_Id varchar(100) primary key,
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published_Date datetime
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()

    com_list =[]
    db=client['youtube_data']
    collection1 = db["channel_details"]
    for com_data in collection1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])     
    df2 = pd.DataFrame(com_list)  

    for index,row in df2.iterrows():
                insert_query = '''insert into comments(Video_Id,
                                                Comment_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published_Date)
                                    
                                                values(%s,%s,%s,%s,%s)'''
        
                values=(row['Video_Id'],
                row['Comment_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published_Date'])

                cursor.execute(insert_query,values)
                mydb.commit()

def tables():
    channel_table()
    video_table()
    comment_table()

    return 'table created'

def show_channel_table():
    ch_list =[]
    db=client['youtube_data']
    collection1 = db["channel_details"]
    for ch_data in collection1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])     
    df = st.dataframe(ch_list)  

    return df

def show_video_table():
    vi_list =[]
    db=client['youtube_data']
    collection1 = db["channel_details"]
    for vi_data in collection1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])     
    df1 = st.dataframe(vi_list)  

    return df1

def show_comment_table() :
    com_list =[]
    db=client['youtube_data']
    collection1 = db["channel_details"]
    for com_data in collection1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])     
    df2 = st.dataframe(com_list)  

    return df2

with st.sidebar:
    menu = option_menu(
        menu_title =None,
        options = ["Home","Get Data","Channel Details","Query"]
    )
if menu =="Home":    
    st.title(":red[#Youtube] :blue[ Data Harvesting And Warehousing]")
    
    multi = '''In this web application we can access and analyze data from multiple YouTube channels.'''
    st.markdown(multi)
    
if menu == "Get Data":
    st.title("Scrapping The Data")
    channel_id = st.text_input("Enter the Channel Id")

    if st.button("collect and store data"):
        ch_ids = []
        db = client['youtube_data']
        callection1= db["channel_information"]
        for ch_data in callection1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])

        if channel_id in ch_ids:
            st.success("Given channel id datails already exists")

        else:
            insert = channel_details(channel_id)
            st.success(insert)

    if st.button("Transfer the data to Sql"):
        Table =tables()
        st.success(Table)

if menu == "Channel Details":

    st.title("Channel Informations")

    show_table=st.radio("Select the table for view",("CHANNELS","VIDEOS","COMMENTS"))

    if show_table == "CHANNELS":
        show_channel_table()

    elif show_table == "VIDEOS":
        show_video_table()

    elif show_table == "COMMENTS":
        show_comment_table()

if menu == "Query":

    st.title("Query Data")
    mydb = mysql.connector.connect( host="localhost",user="root",password="sk23",database='youtube_data')
    cursor = mydb.cursor(buffered=True)

    Question = st.selectbox("Select question",("1. What are the names of all the videos and their corresponding channels?",
                                                    "2. Which channels have the most number of videos, and how many videos do they have?",
                                                    "3. What are the top 10 most viewed videos and their respective channels?",
                                                    "4. How many comments were made on each video, and what are their corresponding video names?",
                                                    "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                    "6. What is the total number of likes for each video, and what are their corresponding video names?",
                                                    "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                    "8. What are the names of all the channels that have published videos in the year 2022?",
                                                    "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                    "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))


    if Question =="1. What are the names of all the videos and their corresponding channels?":
        query1 = '''select channel_name as channelname,video_name as videoname from videos'''
        cursor.execute(query1)
        mydb.commit()
        t1 = cursor.fetchall()
        df1 = pd.DataFrame(t1,columns=["Channel Name","Video_Name"])
        st.write(df1)


    elif Question == "2. Which channels have the most number of videos, and how many videos do they have?":
        query2 = '''select channel_name as channelname,total_videos as numberofvideos from channels 
                    order by total_videos desc'''
        cursor.execute(query2)
        mydb.commit()
        t2 = cursor.fetchall()
        df2 = pd.DataFrame(t2,columns=["Channel Name","Number of Videos"])
        st.write(df2)


    elif Question == "3. What are the top 10 most viewed videos and their respective channels?":
        query3 = '''select view_count as views,video_name as videoname , channel_name as channelname from videos 
                    where view_count is not null order by view_count desc limit 10 '''
        cursor.execute(query3)
        mydb.commit()
        t3 = cursor.fetchall()
        df3 = pd.DataFrame(t3,columns=["Views","Video Name","Channel Name"])
        st.write(df3)


    elif Question == "4. How many comments were made on each video, and what are their corresponding video names?":
        query4 = '''select comment_count as comments,video_name as videoname from videos
                    where comment_count is not null '''
        cursor.execute(query4)
        mydb.commit()
        t4 = cursor.fetchall()
        df4 = pd.DataFrame(t4,columns=["Comments Count","Video Name"])
        st.write(df4)


    elif Question == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
        query5 = '''select like_count as likes,video_name as videoname,channel_name as channelname from videos
                    where like_count is not null order by like_count desc'''
        cursor.execute(query5)
        mydb.commit()
        t5 = cursor.fetchall()
        df5 = pd.DataFrame(t5,columns=["Likes Count","Video Name","Channel Name"])
        st.write(df5)


    elif Question == "6. What is the total number of likes for each video, and what are their corresponding video names?":
        query6 = '''select like_count as likes,video_name as videoname from videos'''
        cursor.execute(query6)
        mydb.commit()
        t6 = cursor.fetchall()
        df6 = pd.DataFrame(t6,columns=["Likes Count","Video Name"])
        st.write(df6)


    elif Question == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
        query7 = '''select channel_views as totalviews,channel_name as channelname from channels'''
        cursor.execute(query7)
        mydb.commit()
        t7 = cursor.fetchall()
        df7 = pd.DataFrame(t7,columns=["Total Views","Channel Name"])
        st.write(df7)


    elif Question == "8. What are the names of all the channels that have published videos in the year 2022?":
        query8 = '''select channel_name as channelname,video_name as videoname,published_date as uploaddate from videos
                    where extract(year from published_date)= 2022'''
        cursor.execute(query8)
        mydb.commit()
        t8 = cursor.fetchall()
        df8 = pd.DataFrame(t8,columns=["Channel Name","Video Name","Published Date"])
        st.write(df8)


    elif Question ==  "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query9 = '''select channel_name as channelname, AVG(duration) as averageduration from videos group by channel_name'''
        cursor.execute(query9)
        mydb.commit()
        t9 = cursor.fetchall()
        df9 = pd.DataFrame(t9,columns=["Channel Name","Average Duration"])

        T9 =[]
        for index,row in df9.iterrows():
            channel_tittle = row["Channel Name"]
            average_duration =row["Average Duration"]
            average_duration_str = str(average_duration)
            T9.append(dict(Channel_Name =channel_tittle,Average_Duration =average_duration_str))
        Df9 = pd.DataFrame(T9)
        st.write(Df9)


    elif Question ==  "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
        query10 = '''select video_name as videoname,comment_count as comments,channel_name as channelname from videos 
                    where comment_count is not null order by comment_count desc'''
        cursor.execute(query10)
        mydb.commit()
        t10 = cursor.fetchall()
        df10 = pd.DataFrame(t10,columns=["Video Name","Comment Counts","Channel Name"])
        st.write(df10)
