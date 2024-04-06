WeChat: cstutorcs
QQ: 749389476
Email: tutorcs@163.com
import requests
from bs4 import BeautifulSoup
import re
import pymongo
import mysql.connector
from cassandra.cluster import Cluster

url = "https://old.reddit.com"
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["ass_db"]
reddits = mydb['reddits']

articles = []

# mysql
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "admin123"
}


def init_mysql():
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        create_database_query = f"CREATE DATABASE IF NOT EXISTS ass_db"
        cursor.execute(create_database_query)
        cursor.execute(f"USE ass_db")
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS reddits (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255),
                poster_name VARCHAR(255),
                post_time VARCHAR(255),
                subreddit VARCHAR(255),
                initial_post TEXT
            )
            """
        cursor.execute(create_table_query)
    except mysql.connector.Error as error:
        print(error)
    finally:
        print("Create mysql database and table")
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

def init_cassandra():
    try:
        cluster = Cluster(contact_points=['127.0.0.1'], port=9042)
        session = cluster.connect()
        keyspace_name = "reddits"
        keyspace_query = f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        session.execute(keyspace_query)
        session.set_keyspace(keyspace_name)
        table_name = "reddits"
        table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                title text primary key,
                poster_name text,
                post_time text,
                subreddit text,
                initial_post text
            )
        """
        session.execute(table_query)
        cluster.shutdown()
        print("cassandra database created.")
    except Exception as e:
        print(e)
        pass

def insert_to_mysql(data):
    try:
        db_config['database'] = 'ass_db'
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        insert_query = "INSERT INTO reddits (title, poster_name, post_time, subreddit, initial_post) VALUES (%s, %s, %s, %s, %s)"
        values = (data["title"], data["poster_name"], data["post_time"], data["subreddit"], data["initial_post"])
        cursor.execute(insert_query, values)
        connection.commit()
        print(f"Inserted to mysql: {data['title']}")
    except:
        pass

def insert_to_mongodb(data):
    try:
        reddits.insert_one(data)
        print(f"Inserted to mongodb: {data['title']}")
    except:
        pass

def insert_to_cassandra(data):
    try:
        cluster = Cluster(contact_points=['127.0.0.1'], port=9042)
        session = cluster.connect()
        insert_query = "INSERT INTO reddits.reddits (title, poster_name, post_time, subreddit, initial_post) VALUES (%s, %s, %s, %s, %s)"
        session.execute(insert_query, [data["title"], data["poster_name"], str(data["post_time"]), data["subreddit"], data["initial_post"]])
        print(f"Inserted to cassandra: {data['title']}")
    except Exception as e:
        print(e)
        pass

def crawl_article_detail(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")    
    article_area = soup.find("div", id="siteTable")
  
    article_content = ""
    contents = article_area.find_all("div", class_="md")
    for content in contents:
        content_text = content.find("p").text
        article_content = article_content + "\n" + content_text
    return article_content    


def crawl_articles(url, article_count):
    count = article_count
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    articles = soup.find_all("div", class_="top-matter")

    for article in articles:
        if count >= 100:
            return
        title = article.find("a", class_="title").text
        
        detail_link = article.find("a", class_="title")['href']
        
        if re.match("^/r/.+", detail_link):
            try:
                count += 1
                poster_name = article.find("a", class_="author").text
                post_time = article.find("time")["datetime"]
                subreddit = article.find("a", class_="subreddit").text
                initial_post = crawl_article_detail(url + article.find("a", class_="title")['href'])
                record = {
                    "title": title,
                    "poster_name": poster_name,
                    "post_time": post_time,
                    "subreddit": subreddit,
                    "initial_post": initial_post
                }
                insert_to_mysql(record)
                insert_to_mongodb(record)
                insert_to_cassandra(record)
                
            except:
                pass    

    if count < 100:
        next_button = soup.find("span", class_="next-button")
        next_page = next_button.find("a")
        if next_page:
            next_url = next_page['href']
            if next_url:
                crawl_articles(next_url, count)          

        

init_mysql()
init_cassandra()
crawl_articles(url, 0)
