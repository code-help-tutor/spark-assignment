WeChat: cstutorcs
QQ: 749389476
Email: tutorcs@163.com
import requests
from bs4 import BeautifulSoup
import re
import pymongo
import mysql.connector
from cassandra.cluster import Cluster

url = "https://www.bestbuy.ca/api/v2/json/search?categoryid=20001&lang=en-CA&page=2&pageSize=250"
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["ass_db"]
goods = mydb['goods']

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
            CREATE TABLE IF NOT EXISTS goods (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                price DECIMAL(10, 2),
                description TEXT,
                categoryName VARCHAR(255),
                thumbnailImage VARCHAR(255)
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
        keyspace_name = "goods"
        keyspace_query = f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        session.execute(keyspace_query)
        session.set_keyspace(keyspace_name)
        table_name = "goods"
        table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                name text,
                price decimal,
                description text,
                categoryName text,
                thumbnailImage text
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
        insert_query = "INSERT INTO goods (name, price, description, categoryName, thumbnailImage) VALUES (%s, %s, %s, %s, %s)"
        values = (data['name'], data['price'], data['description'], data['categoryName'], data['thumbnailImage'])
        cursor.execute(insert_query, values)
        connection.commit()
        print(f"Inserted to mysql: {data['name']}")
    except:
        pass

def insert_to_mongodb(data):
    try:
        goods.insert_one(data)
        print(f"Inserted to mongodb: {data['name']}")
    except:
        pass

def insert_to_cassandra(data):
    try:
        cluster = Cluster(contact_points=['127.0.0.1'], port=9042)
        session = cluster.connect()
        insert_query = f"""
           INSERT INTO goods.goods (id, name, price, description, categoryName, thumbnailImage)
           VALUES (uuid(), %s, %s, %s, %s, %s)
           """
        session.execute(insert_query, (data['name'], data['price'], data['description'], data['categoryName'], data['thumbnailImage']))
        print(f"Inserted to cassandra: {data['name']}")
    except Exception as e:
        print(e)
        pass


def crawl_goods(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    json_data = response.json()
    products = json_data['products']

    for product in products[0:200]:
        try:
            data = {
                'name': product['name'],
                'price': product['salePrice'],
                'description': product['shortDescription'],
                'categoryName': product['categoryName'],
                'thumbnailImage': product['thumbnailImage']
            }
            insert_to_mysql(data)
            insert_to_cassandra(data)
            insert_to_mongodb(data)
        except Exception as e:
            print(e)

init_mysql()
init_cassandra()
crawl_goods(url)
