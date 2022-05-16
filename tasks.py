from celery import Celery
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime

app = Celery('tasks')

HN_URL = ["https://news.ycombinator.com/","https://news.ycombinator.com/news?p=2"]

@app.task
def save(contentlist):
    timestmp = datetime.now().strftime('%Y%m%d_%H%M%S')
    fname = 'articles_{}.json'.format(timestmp)
    with open(fname ,'w') as output:
        json.dump(contentlist,output)

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



# if __name__ == "__main__":
#     for url in HN_URL:
#         hn(url)