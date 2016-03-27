import psycopg2
import urllib2
import xml.etree.ElementTree as etree
from bs4 import BeautifulSoup
import requests




conn = psycopg2.connect(database="NewsSource", user="rakesh", password="davps2005", host="newdb.cnceaogjppz8.us-west-2.rds.amazonaws.com", port="5432")
cur = conn.cursor()

cur.execute("select articletext from articlestable where id = 8609")

row = cur.fetchall()
for rows in row:
	print rows[0]