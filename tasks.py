from celery import Celery
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
import os
import psycopg2
import json
from NoDataException import NoDataException


def postgresconn():
    try:
        conn = psycopg2.connect(host='localhost',database= os.environ['dbname'],user=os.environ['app_user'],password=os.environ['app_password'])
        return conn
    except Exception as e:
        print(e)


app = Celery('tasks')

HN_URL = ["https://news.ycombinator.com/","https://news.ycombinator.com/news?p=2"]

@app.task
def save(contentlist):
    timestmp = datetime.now().strftime('%Y%m%d_%H%M%S')
    conn = postgresconn()
    cursor = conn.cursor()
    prefix = f'INSERT INTO {os.environ["schemaname"]}.{os.environ["tablename"]}(data) VALUES'
    stmt = ''
    for content in contentlist:
        content['title'] = content['title'].replace("'","''")
        json_object = json.dumps(content)
        stmt += '(' + "\'" + json_object + "\'" + ')' ','
    sqlstmt = prefix + stmt[0:-1]
    res = cursor.execute(sqlstmt)
    conn.commit()
    cursor.close()
    conn.close()
    return

    

#Scraping
@app.task
def hn(url):
    try:
        res = requests.get(url)
        soup = BeautifulSoup(res.content,'html.parser')
        articles = soup.findAll(class_='titlelink')
        scores = soup.findAll(class_='subtext')
        
        
        content = []
        for i in range(len(articles)):
            todict = {}
            todict['created_at'] = str(datetime.now())
            todict['source'] = 'HN'
            todict['title'] = articles[i].string
            todict['link'] = articles[i]['href']
            todict['score'] = scores[i].span.text
            # cleaning
            if 'points' not in todict['score']:
                todict['score'] = 0
            content.append(todict)

        return save(content)
    except Exception as e:
        print(e)

#RSS 
#XML Parsing
#https://www.reddit.com/r/pathogendavid/comments/tv8m9/pathogendavids_guide_to_rss_and_reddit/
@app.task
def reddit():
    url = "https://www.reddit.com/r/programming/new/.rss?sort=new"
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0"}
    res = requests.get(url,headers=headers)
    soup = BeautifulSoup(res.content,features='xml')
    entries = soup.findAll('title')

    content = soup.findAll("content")
    links = []
    articles = []
    for cont in content:
        htmlstring = cont.string
        blob = re.findall('(<span><a href=".*[link])',htmlstring)
        tags = re.findall('<.*>',''.join(blob))
        actuallink = ''.join(tags).split("\"")
        links.append(actuallink[1])

    # As first entry is just meta information
    entries = entries[1:]

    for i in range(len(entries)):
        todict = {}
        todict['title'] = entries[i].text
        todict['link'] = links[i]
        todict['source'] = 'Reddit programming'
        articles.append(todict)

    return save(articles)
        