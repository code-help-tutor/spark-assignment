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
goods = mydb['goods']

db_config = {
    "host": "localhost",
    "user": "root",
    "password": "admin123",
    "database": "ass_db"
}

def query_mysql():
    print("\n-------------------Mysql Query result--------------------------")
    # Mysql
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        category_count_query = "SELECT categoryName, COUNT(*) AS count FROM goods GROUP BY categoryName ORDER BY count DESC"
        cursor.execute(category_count_query)
        category_counts = cursor.fetchall()
        print("Category and count: ")
        for category in category_counts:
            print(f"{category[0]} - {category[1]}")

        max_price_query = """
        SELECT name, price
        FROM goods
        WHERE price = (SELECT MAX(price) FROM goods)
        """
        cursor.execute(max_price_query)
        max_price_result = cursor.fetchone()
        max_price_name, max_price = max_price_result

        min_price_query = """
        SELECT name, price
        FROM goods
        WHERE price = (SELECT MIN(price) FROM goods)
        """
        cursor.execute(min_price_query)
        min_price_result = cursor.fetchone()
        min_price_name, min_price = min_price_result
        print(f"\nThe most expensive good: {max_price_name}, price: {max_price}")
        print(f"The most cheapest good: {min_price_name}, price: {min_price}")

        average_price_query = "SELECT AVG(price) FROM goods"
        cursor.execute(average_price_query)
        average_price = cursor.fetchone()[0]
        print(f"\nAverage Price of Goods: {average_price:.2f}")
        print("\n----------------------------------------------------------------")
    except Exception as e:
        print(e)


def query_mongo():
    print("\n---------------------------------------MongoDB Query result---------------------------")
    pipeline = [
        {
            "$group": {
                "_id": "$categoryName",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        }
    ]
    category_counts = list(goods.aggregate(pipeline))
    print("Category and count: ")
    for category in category_counts:
        print(f"{category['_id']} - {category['count']}")

    max_price_document = goods.find_one(sort=[("price", pymongo.DESCENDING)])
    max_price_name = max_price_document['name']
    max_price = max_price_document['price']

    min_price_document = goods.find_one(sort=[("price", pymongo.ASCENDING)])
    min_price_name = min_price_document['name']
    min_price = min_price_document['price']
    print(f"\nThe most expensive good: {max_price_name}, price: {max_price}")
    print(f"The most cheapest good: {min_price_name}, price: {min_price}")

    pipeline = [
        {
            "$group": {
                "_id": None,
                "average_price": {"$avg": "$price"}
            }
        }
    ]
    result = list(goods.aggregate(pipeline))
    avg_price = result[0]['average_price']
    print(f"\nAverage Price of Goods: {avg_price:.2f}")
    print("\n----------------------------------------------------------------")


def query_cassandra():
    print("\nCassandra Query result: ")

    cluster = Cluster(contact_points=['127.0.0.1'], port=9042)
    session = cluster.connect("goods")

    select_all_query = "SELECT * FROM goods"
    result = session.execute(select_all_query)

    categories = {}

    min_price_name = None
    min_price = 10000000000
    max_price_name = None
    max_price = 0
    sum_price = 0
    count = 0

    for item in result:
        category = item.categoryname
        if category in categories:
            categories[category] += 1
        else:
            categories[category] = 1

        sum_price = sum_price + item.price
        count += 1
        if item.price < min_price:
            min_price_name = item.name
            min_price = item.price
        if item.price > max_price:
            max_price_name = item.name
            max_price = item.price

    print("Category and count: ")
    for category in categories:
        print(f"{category} - {categories[category]}")



    print(f"\nThe most expensive good: {max_price_name}, price: {max_price}")
    print(f"The most cheapest good: {min_price_name}, price: {min_price}")
    avg_price = sum_price / count
    print(f"\nAverage Price of Goods: {avg_price:.2f}")
    print("\n----------------------------------------------------------------")

query_mysql()
query_mongo()
query_cassandra()



