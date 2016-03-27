import psycopg2
import os
from os import listdir
from os import getcwd

filelist =[]
folderpath='hdfs://ec2-54-213-170-202.us-west-2.compute.amazonaws.com/user/root/classified_data'
conn = psycopg2.connect(database="NewsSource", user="rakesh", password="davps2005", host="newdb.cnceaogjppz8.us-west-2.rds.amazonaws.com", port="5432")
cur = conn.cursor()
update = 'update articlestable set classifiedcategory=%s where id=%s'
for f in listdir(folderpath):
	  filelist.append(f)
for item in filelist:
	  path=folderpath+"/"+item
	  fi=open(path,'r')
	  category=fi.read()
	  id=item;
	  print category
	  print item
	  data = (category,id)
	  cur.execute(update,data)
	  conn.commit()
	  fi.close()
	  os.remove(fi.name)
