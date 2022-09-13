
import time
import pandas as pd
import json
import mysql.connector as conn
import urllib.request
import base64
import pymongo
from flask import Flask, render_template, request,jsonify
from flask_cors import CORS,cross_origin
import requests
from urllib.request import urlopen as uReq


# <input type="text" name="content" id="content">
import os
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.service import Service
from pytube import YouTube
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


gauth = GoogleAuth()
drive = GoogleDrive(gauth)


folder = '1Mec5mLuRkPYOy9Dze0KaWz9P5lOo4cUr'
vid_directory = "./videos"

options = Options()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument("--headless")
options.add_argument("start-maximized")
options.add_argument('--window-size=1920,1080')
options.add_argument("--start-maximized")

caps = DesiredCapabilities().CHROME
caps["pageLoadStrategy"] = "eager"

app = Flask(__name__)

client = pymongo.MongoClient(
        "mongodb+srv://sinara:rootroot@sincluster.0lcik.mongodb.net/?retryWrites=true&w=majority")
mongo_db = client.test

mysql_db = conn.connect(host="localhost", user="root", passwd="rootroot", allow_local_infile=True)
crs = mysql_db.cursor()

def data_load_mongodb(df_mongo):


    attr_data = df_mongo.to_json('YT_Comment.json', orient="records")

    # ----------(5)Load Attribute data to MongoDB ---------------
    with open('YT_Comment.json', 'r') as file:
        file_data = json.load(file)

    database = client['YT_Details']
    collection = database["YT_Comment_Details"]
    collection.insert_many(file_data)


def data_load_sql():

    #Create Database and Table
    db_crt_sql = "CREATE DATABASE IF NOT EXISTS YT_DTLS"
    crs.execute(db_crt_sql)
    mysql_db.commit()

    sql_stmt = 'SET GLOBAL local_infile = TRUE'
    crs.execute(sql_stmt)

    # pip install csvkit
    # csvsql --dialect mysql --snifflimit 100000  /Users/nikhilgopalakrishnan/Documents/Sinara/FSDS/"FitBit_data.csv" > /Users/nikhilgopalakrishnan/Documents/Sinara/FSDS/fitbit_table.sql
    cre_sql = "CREATE TABLE  if not exists YT_DTLS.`YT_Channel_Details` (    \
    	        `Channel_Owner` VARCHAR(10) NOT NULL, \
	            `Link` VARCHAR(100) NOT NULL,  \
	            `Title` VARCHAR(100) NOT NULL,   \
	            likes DECIMAL(38, 0) NOT NULL,   \
	            `ThumbNail` VARCHAR(100) NOT NULL,   \
	            `Comments_Cnt` VARCHAR(20) NOT NULL,  \
                `Comment_Users_lst` VARCHAR(500) NOT NULL, \
                `GLink` VARCHAR(100) NOT NULL   \
    )"

    crs.execute(cre_sql)

    # ----------(2)Bulk Load---------------
    attr = "Load data local infile 'YT_MYSQL_Dtls.csv' into table YT_DTLS.`YT_Channel_Details` fields terminated by ',' ENCLOSED BY '\"' ESCAPED BY '\\\\' Lines terminated by '\n'  ignore 1 lines (Channel_Owner, Link, Title,	likes,	ThumbNail, Comments_Cnt, Comment_Users_lst,	GLink)"

    crs.execute(attr)
    mysql_db.commit()




def downloadAndupload_videos(url: str, folder_id, outpath: str = "./"):

    yt = YouTube(url)

    for f in os.listdir(vid_directory):
        vid_fl = yt.streams.filter(file_extension="mp4").get_by_resolution("360p").download(outpath)

    vid_fl=vid_fl.split('/')[-1]

    #for f in os.listdir(vid_directory):
    filename = os.path.join(vid_directory, vid_fl)
    gfile = drive.CreateFile({'title': vid_fl,
                              'mimeType': "video/mp4",
                              'parents': [{"kind": "drive#fileLink", "id": folder_id}]})
    gfile.SetContentFile(filename)
    #print(filename)
    gfile.Upload()

    permission = gfile.InsertPermission({
        'type': 'anyone',
        'value': 'anyone',
        'role': 'reader'})

    link = gfile['alternateLink']
    return link


def getCommentDetails(url_dtls, wd, sleep_between_interactions):
    wait = WebDriverWait(wd, 20)

    comment_list = []
    i = 1


    for url in list(url_dtls):
        #print("Comments Section")
        #print("Video {}".format(i))
        #print("url is " + url)
        # Extract dates from for each user on a page
        wd.get(url)
        wd.maximize_window()
        v_link = url

        last_height = wd.execute_script("return document.documentElement.scrollHeight")

        while True:
            #print("Here")
            time.sleep(5)
            # Scroll down to bottom
            wd.execute_script("window.scrollTo(0, arguments[0]);", last_height)
            # Wait to load page
            time.sleep(2)

            # Calculate new scroll height and compare with last scroll height
            new_height = wd.execute_script("return document.documentElement.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        try:
            own_path = '//*[@id="channel-name"]//*[@id="text"]/a'
            v_channel_owner = wait.until(
                EC.visibility_of_element_located((By.XPATH, own_path))).text
        except:
            v_channel_owner = ''

        #print(v_channel_owner)

        time.sleep(2)
        try:
            comm_thumbNail = wd.find_elements_by_xpath(
                '//*[@id="contents"]/ytd-comment-thread-renderer//*[@id="author-thumbnail"]//*[@id="img"]')

            if comm_thumbNail == []:
                #print("Here11")
                #time.sleep(5)
                #wd.execute_script("window.scrollTo(0, 300;")
                comm_button = wd.find_element_by_xpath('//*[@id="comments-button"]')
                comm_button.click()
                time.sleep(2)

                last_height = wd.execute_script("return document.documentElement.scrollHeight")

                comm_thumbNail = wd.find_elements_by_xpath('//*[@id="author-thumbnail"]//a/yt-img-shadow//*[@id="img"]')
                #time.sleep(2)


        except:
            try:
                #print("Here1")
                #time.sleep(5)
                comm_button = wd.find_element_by_xpath('//*[@id="comments-button"]')
                comm_button.click()
                time.sleep(2)
                comm_thumbNail = wd.find_elements_by_xpath('//*[@id="author-thumbnail"]//a/yt-img-shadow//*[@id="img"]')
                #time.sleep(2)
            except:
                comm_thumbNail = ''

            # comm_thumbNail=''

        #print(comm_thumbNail)

        try:
            comm_nm = wait.until(EC.presence_of_all_elements_located((By.XPATH,
                '//*[@id="header-author"]/h3//*[@id="author-text"]/span')))
            time.sleep(2)

        except:
            comm_nm = ''

        #print(comm_nm)
        try:
            comm_txt = wait.until(EC.presence_of_all_elements_located((By.XPATH,
                                        "//*[@id='contents']/ytd-comment-thread-renderer//yt-formatted-string[@class='style-scope ytd-comment-renderer' and @id='content-text'][@slot='content']")))

            time.sleep(2)

        except:
            comm_txt=''

        #print(comm_txt)

        comm_lst=[]
        comm_lst.append([comm.get_attribute("innerHTML").strip() for comm in comm_nm])
        #print(comm_lst, comm_lst[0])

        if comm_lst[0]== []:
            comm_lst.append([comm.text for comm in comm_nm])
            #print(comm_lst, comm_lst[0])

        cnt = 1



        for comm_img, comm_t, comm_n in zip(comm_thumbNail, comm_txt, comm_nm):
            img_url=''
            comm_name=''
            b64_string=''
            comm_text=''

            # comm users Thumbnail
            #print(comm_img.get_attribute("src"))
            img_url = comm_img.get_attribute("src")

            # comm text
            comm_text=comm_t.text
            #print(comm_t.text)

            # comm uer name
            #print(comm_n.get_attribute("innerHTML").strip())
            comm_name = comm_n.get_attribute("innerHTML").strip()

            # comm users Thumbnail to base64
            if comm_name != '':
                img_nm = comm_name
            else:
                img_nm = "Cmnt_cnt_" + f"{cnt}"

            if img_url is not None :
                urllib.request.urlretrieve(img_url, f"./{img_nm}.jpg")
                with open(f"{img_nm}.jpg", "rb") as img_file:
                    b64_string = base64.b64encode(img_file.read())

            #print(b64_string)



            #Final comment details
            obj = {
                "Channel_Owner": v_channel_owner,
                "Link": v_link,
                "Comment_Text": comm_t.text,
                "Comment_Users": comm_name,
                #"Comment_ThumbNail": comm_img.get_attribute("src"),
                "Comment_ThumbNail" : img_url,
                "Comment_ThumbNail_base64": b64_string,
                "Comment_Users_lst": comm_lst[0]
            }
            comment_list.append(obj)
        #downloadAndupload_videos(url,vid_directory)

        i += 1






    return comment_list


def getChannelDetails(url_dtls, wd, folder_id, sleep_between_interactions):


    wait = WebDriverWait(wd, 20)

    details = []
    i = 1



    for url in list(url_dtls):
        #print("Video {}".format(i))
        #print("url is " + url)
        # Extract dates from for each user on a page
        wd.get(url)
        wd.maximize_window()
        v_link = url

        title_xpath = "//div[@class='style-scope ytd-video-primary-info-renderer']/h1"
        alternative_title_1= "//*[@id='title']/h1"
        alternative_title_2='//*[@id="overlay"]/ytd-reel-player-header-renderer/h2/yt-formatted-string'
        v_title = ""
        g_link = ''
        try:
            time.sleep(2)
            v_title = wait.until(EC.visibility_of_element_located((By.XPATH, title_xpath))).text
        except Exception as e:
            #print(str(e))
            try:
                v_title = wait.until(EC.visibility_of_element_located((By.XPATH, alternative_title_1))).text
            except:
                try:
                    v_title = wait.until(EC.visibility_of_element_located((By.XPATH, alternative_title_2))).text
                except:
                    v_title == ""

        #print("Title is " + v_title)
        #time.sleep(2)
        likes_xpath = '(//div[@id="top-level-buttons-computed"]//*[contains(@aria-label," likes")])[last()]'

        likes_alternate_path = '//*[@id="like-button"]//*[contains(@aria-label, "like")][1]'
        likes_alternate_path_1 ='//*[@id="text"][contains (@aria-label, "like")]'
        try:
            time.sleep(2)
            v_like = wait.until(EC.visibility_of_element_located((By.XPATH,likes_xpath))).text
        except Exception as e:
            try:

                v_like = wait.until(EC.visibility_of_element_located((By.XPATH, likes_alternate_path))).text
            except:
                try:
                    v_like = wait.until(EC.visibility_of_element_located((By.XPATH, likes_alternate_path_1))).text
                except:
                    v_like = ""

        #print(v_like)

        #time.sleep(25)
        img_path = '//*[@id="watch7-content"]/link[2]'
        try:
            #v_image = wait.until(EC.visibility_of_element_located((By.XPATH,img_path))).get_attribute("href")
            time.sleep(2)
            v_image = wd.find_element_by_xpath('//*[@id="watch7-content"]/link[2]').get_attribute("href")
        except Exception as e:

            v_image = ''
        #print(v_image)

        try:
            #time.sleep(10)
            time.sleep(2)
            subscribe = wait.until(
             EC.visibility_of_element_located((By.XPATH, "//yt-formatted-string[text()='Subscribe']")))
            wd.execute_script("arguments[0].scrollIntoView(true);", subscribe)
            #time.sleep(5)
            v_commentscnt = wait.until(
             EC.visibility_of_element_located((By.XPATH, "//h2[@id='count']/yt-formatted-string"))).text
        except Exception as e:
            try:
                comment_path = '//*[@id="text"][@class="style-scope ytd-button-renderer"][1]'
                v_commentscnt = wait.until(
                    EC.visibility_of_element_located((By.XPATH,comment_path))).text
            except:
                try:
                    comment_path = '//*[@id="count"]/yt-formatted-string/span[1]'
                    v_commentscnt = wait.until(
                        EC.visibility_of_element_located((By.XPATH, comment_path))).get_attribute("innerHTML")
                except:
                    v_commentscnt=""

        #print(v_commentscnt)

        try:
            time.sleep(2)
            own_path = '//*[@id="channel-name"]//*[@id="text"]/a'
            v_channel_owner = wait.until(
                EC.visibility_of_element_located((By.XPATH, own_path))).text
            #v_channel_owner = wait.until(EC.visibility_of_element_located(By.XPATH,'//*[@id="channel-name"]//*[@id="text"]/a')).text
            #v_channel_owner=wd.find_element_by_xpath('//*[@id="channel-name"]//*[@id="text"]/a').text
        except :
            v_channel_owner = ''

        #print(v_channel_owner)

        g_link = downloadAndupload_videos(url, folder_id, vid_directory)

        #print(g_link)

        obj = {
            "Channel_Owner": v_channel_owner,
            "Link": v_link,
            "Title": v_title,
            "GLink": g_link,
            "likes": v_like,
            "ThumbNail": v_image,
            "Comments_Cnt": v_commentscnt
            #"Comments_Users": comm_usrs

        }
        details.append(obj)
        i += 1

    return details

def fetch_video_urls(query: str, max_links_to_fetch: int, wd: webdriver, sleep_between_interactions: int = 1):
    # def scroll_to_end(wd):
    #     wd.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
    #     time.sleep(sleep_between_interactions)

    # build the Youtube query

    baseUrl = "https://youtube.com/"



    wd.get(f"{baseUrl}/search?q={query}&sp=EgIQAg%253D%253D")
    time.sleep(sleep_between_interactions)



    vid_cnt = 0
    last_height = wd.execute_script("return document.documentElement.scrollHeight")

    thumbnail = wd.find_element_by_xpath('//*[@id="avatar"]//img[@id="img"]')
    #//*[@id="endpoint"][@class ="yt-simple-endpoint style-scope ytd-guide-entry-renderer"][contains ( @ href, "user")]
    try:
        thumbnail.click()
        time.sleep(sleep_between_interactions)
    except Exception:
        pass
    vid_tab = wd.find_element_by_xpath('//*[@id="tabsContent"]/tp-yt-paper-tab[2]/div')
    try:
        vid_tab.click()
        time.sleep(sleep_between_interactions)
    except Exception:
        pass

    vid_cnt = 0
    last_height = wd.execute_script("return document.documentElement.scrollHeight")
    while True:
        wd.execute_script("window.scrollTo(0, arguments[0]);", last_height)
        user_data = wd.find_elements_by_xpath('//*[@id="video-title"]')
        if len(user_data) >= max_links_to_fetch:
            break
        new_height = wd.execute_script("return document.documentElement.scrollHeight")
        last_height = new_height

    lnks = []


    for i in user_data:
        lnks.append(i.get_attribute('href'))
        vid_cnt += 1
        if vid_cnt >= max_links_to_fetch:
            break

    #print(len(lnks))

    return lnks



def search_and_download(search_term: str, driver_path: str, target_path='./videos', number_vid=1):
    YT_filename = "Details.csv"
    YT_MYSQL_Dtls = "YT_MYSQL_Dtls.csv"
    YT_Mongo_Dtls = "YT_Mongo_Dtls.csv"


    file1 = drive.CreateFile({'title': 'YT_Videos',
                              'mimeType': "application/vnd.google-apps.folder"})
    file1.Upload()

    folder_id = file1['id']

    res_url = []
    wd = webdriver.Chrome(executable_path=driver_path , desired_capabilities=caps, options=options)

    #for i in range(len(search_list)):
    url = fetch_video_urls(search_term, number_vid, wd=wd, sleep_between_interactions=5)
    res_url.extend(url)

    #res_url=["https://www.youtube.com/shorts/uXEg8TF-2ZA"]
    vidDtls = getChannelDetails(res_url, wd, folder_id, sleep_between_interactions=5)
    df = pd.DataFrame(vidDtls)
    #df.to_csv(YT_filename, index=False)

    vidCommDtls = getCommentDetails(res_url, wd, sleep_between_interactions=5)
    wd.quit()
    df_comm = pd.DataFrame(vidCommDtls)
    #df_comm.to_csv(YT_Comm_filename, index=False)
    df_final = pd.merge(df, df_comm, how="inner")

    df_final.to_csv(YT_filename, index=False)

    df_my_sql = df_final[['Channel_Owner','Link','Title','likes','ThumbNail','Comments_Cnt','Comment_Users_lst','GLink']].copy()
    df_my_sql = df_my_sql.loc[df_my_sql.astype(str).drop_duplicates().index]

    df_my_sql.to_csv(YT_MYSQL_Dtls, index=False)
    data_load_sql()

    df_mongo = df_final[
        ['Channel_Owner', 'Link', 'Comment_Users', 'Comment_Text', 'Comment_ThumbNail_base64']].copy()
    df_mongo = df_mongo.loc[df_mongo.astype(str).drop_duplicates().index]
    #df_mongo.drop_duplicates()
    df_mongo.to_csv(YT_Mongo_Dtls, index=False)

    #df_mongo = pd.read_csv('YT_Mongo_Dtls.csv')
    #print("Mongo Load")
    data_load_mongodb(df_mongo)
    #df_final.to_html("Table.htm")
    return df_final

@app.route('/',methods=['GET'])  # route to display the home page
@cross_origin()
def homePage():
    return render_template("index.html")
@app.route('/review',methods=['POST','GET']) # route to show the review comments in a web UI
@cross_origin()
def index():
    if request.method ==\
            'POST':
        try:
            #print("I am Here")

            if request.form['submit_button'] == 'Tulesko':
                text_box_value = 'Tulesko'
            elif request.form['submit_button'] == 'mysirg':
                text_box_value = 'mysirg'
            elif request.form['submit_button'] == 'Krish Naik':
                text_box_value = 'Krish Naik'
            elif request.form['submit_button'] == 'hitesh choudhary':
                text_box_value = 'hitesh choudhary'
            else:
                text_box_value = ''


            DRIVER_PATH = './chromedriver'

            df_final = search_and_download(search_term=text_box_value, driver_path=DRIVER_PATH, number_vid=50)
            #df_final.to_html('table.html')
            return render_template('results.html', reviews=[df_final.to_html()], titles=[''])

        except Exception as e:
            #print('The Exception message is: ',e)
            return 'something is wrong'

    else:
        return render_template('index.html')

if __name__ == "__main__":
    app.run(debug=True)
