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

db_config = {
    "host": "localhost",
    "user": "root",
    "password": "admin123",
    "database": "ass_db"
}

def query_mysql():
    print("\nMysql Query result: ")
    # Mysql
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        count_query = "SELECT COUNT(*) FROM reddits"
        cursor.execute(count_query)
        article_count = cursor.fetchone()[0]
        print(f"Article count is {article_count}")

        non_empty_query = "SELECT COUNT(*) FROM reddits WHERE initial_post IS NOT NULL AND initial_post != ''"
        cursor.execute(non_empty_query)
        non_empty_count = cursor.fetchone()[0]

        empty_query = "SELECT COUNT(*) FROM reddits WHERE (initial_post IS NULL OR initial_post = '')"
        cursor.execute(empty_query)
        empty_count = cursor.fetchone()[0]

        print(f"Mysql Query: The count of initial post is not null: {non_empty_count}")
        print(f"Mysql Query: The count of initial post is null: {empty_count}")
    except Exception as e:
        print(e)


def query_mongo():
    print("\nMongoDB Query result: ")

    article_count = reddits.count_documents({})
    print(f"Article count is {article_count}")

    non_empty_count = reddits.count_documents({"initial_post": {"$exists": True, "$ne": ""}})
    empty_count = reddits.count_documents({"initial_post": {"$exists": True, "$eq": ""}})
    print(f"Mongo Query: The count of initial post is not null: {non_empty_count}")
    print(f"Mongo Query: The count of initial post is null: {empty_count}")

def query_cassandra():
    print("\nCassandra Query result: ")

    cluster = Cluster(contact_points=['127.0.0.1'], port=9042)
    session = cluster.connect("reddits")
    query_all = "SELECT COUNT(*) FROM reddits"
    result_all = session.execute(query_all)
    article_count = result_all.one()[0]
    print(f"Article count is {article_count}")

    query_empty_initial_post = "SELECT COUNT(*) FROM reddits WHERE initial_post=%s ALLOW FILTERING "
    result_empty_initial_post = session.execute(query_empty_initial_post, [""])
    empty_initial_post_count = result_empty_initial_post.one()[0]


    print(f"Cassandra Query: The count of initial post is not null: { article_count - empty_initial_post_count}")
    print(f"Cassandra Query: The count of initial post is null: {empty_initial_post_count}")

query_mysql()
query_mongo()
query_cassandra()



