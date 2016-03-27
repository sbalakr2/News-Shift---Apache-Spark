import psycopg2
import requests
from hdfs import TokenClient


print "Fetching data from the database"
conn = psycopg2.connect(database="NewsSource", user="rakesh", password="davps2005", host="newdb.cnceaogjppz8.us-west-2.rds.amazonaws.com", port="5432")
HDFS_URL = "http://localhost:50070"
cur = conn.cursor()
cur.execute("select id,articletext from articlestable where classifiedcategory IS NULL")
rows = cur.fetchall()

count = 1
for row in rows:
    print "Storing file " + str(count) + " in HDFS"
    client = TokenClient(HDFS_URL, 'crawled_data', root='/user/root')
    client.write(str(row[0]), row[1])
    count +=1
conn.close()
