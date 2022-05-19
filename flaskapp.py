from flask import Flask,render_template
import os
import psycopg2
import json
from flask import jsonify
from NoDataException import NoDataException

app = Flask(__name__)
def postgresconn():
    try:
        conn = psycopg2.connect(host='localhost',database= os.environ['dbname'],user=os.environ['app_user'],password=os.environ['app_password'])
        return conn
    except Exception as e:
        print(e)

@app.route('/')
def index():
    return "Welcome to News Aggregator"

@app.route('/articles',methods=['GET'])
def articles():
    try:
        conn = postgresconn()
        cursor = conn.cursor()
        stmt = f' select cast(row_to_json(row) as text) from (select data from {os.environ["schemaname"]}.{os.environ["tablename"]}) row limit 20;'
        print(stmt)
        cursor.execute(stmt)
        res = cursor.fetchall()
        articles = []
        for row in res:
            jsonifid = json.loads(row[0])
            articles.append(jsonifid['data'])
        cursor.close()
        conn.close()
        print(articles)
        if not res:
            raise NoDataException(f'No Data in the table {os.environ["tablename"]}, of database {os.environ["schemaname"]}')
        
        return render_template('table_template.html',articlelist=articles,colhead=['title','link','source'])
    except NoDataException as e:
       print(e)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    app.run(debug=True)

    

