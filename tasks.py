from celery import Celery
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re

app = Celery('tasks')

HN_URL = ["https://news.ycombinator.com/","https://news.ycombinator.com/news?p=2"]

@app.task
def save(contentlist):
    timestmp = datetime.now().strftime('%Y%m%d_%H%M%S')
    fname = 'articles_{}.json'.format(timestmp)
    with open(fname ,'w') as output:
        json.dump(contentlist,output)

#Scraping
@app.task
def hn(url):
    try:
        res = requests.get(url)
        #soup = BeautifulSoup(res.content,'lxml')
        soup = BeautifulSoup(res.content,'html.parser')
        articles = soup.findAll(class_='titlelink')
        scores = soup.findAll(class_='subtext')
        #print(scores)
        #desired = zip(articles,scores)
        
        #print(articles)
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
        
    
# if __name__ == "__main__":
#     for url in HN_URL:
#         hn(url)
#     reddit()