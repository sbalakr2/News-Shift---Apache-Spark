import psycopg2
import urllib2
import xml.etree.ElementTree as etree
from bs4 import BeautifulSoup
import requests

def crawlpage(articleurl):
	try:
		page = requests.get(articleurl)
		pagesoup = BeautifulSoup(page.text,'html.parser')	
		articletext =''
		#print pagesoup
		if (pagesoup.article == None):
			ptags = pagesoup.find_all('p')
		else:
			ptags = pagesoup.article.find_all('p')				
		
		for p in ptags:
			articletext = articletext+p.text

		#print articletext	

		return articletext
	except:
		articletext = ''
		return articletext


conn = psycopg2.connect(database="NewsSource", user="rakesh", password="davps2005", host="newdb.cnceaogjppz8.us-west-2.rds.amazonaws.com", port="5432")
cur = conn.cursor()
while True:

	insertcouter = 0
	skiped = 0
	notsaved = 0
	cur.execute("select url,rsscategory from rsstable")
	f = open('log.txt',"a+")
	
	

	rows = cur.fetchall()

		
	for row in rows:

		try:
			response = urllib2.urlopen(row[0])
		except Exception as e:
				print "error:  ",str(e),"\n"
				continue

		soup = BeautifulSoup(response.read(),'xml')

		query = "insert into articlestable (title,link,description,content,category,pubdatetime,hashoflink,articletext,crawled,parentcategory) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
		articletext = ''

		for doc in soup.find_all('item'):

			if(doc.title == None):
				title = ''
			else:
				title = doc.title.text
			
			linktag = doc.find(lambda tag: tag.name =='link' and not tag.attrs)
			
			if(linktag == None):
				link = ''
			else:		
				link = linktag.text
				hashoflink = hash(link)
				

			if(doc.description == None):
				description = ''
			else:
				description = doc.description.text
			if(doc.content == None):
				content = ''
			else:
				content = doc.content.text
			
			if(doc.category == None):
				category = ''
			else:
				category = doc.category.text
				print category,"\n"
			if(doc.pubDate == None):
				if(doc.pubdate == None):
					pubdatetime= ''
				else:
					pubdatetime  = pubdate.text
			else:
				pubdatetime = doc.pubDate.text
			parentcategory = row[1]
			#print doc.find(lambda tag: tag.name =='link' and not tag.attrs)	,"\n"

			
			
			cur.execute("select id from articlestable where hashoflink ="+str(hashoflink))
			

			counter = cur.rowcount
			try:

				if(counter == 0 ):
					#crawl the page and insert into db
					articletext = crawlpage(link)

					if(len(articletext) > 0):
						iscrawled = True
						data = (title,link,description,content,category,pubdatetime,hashoflink,articletext,iscrawled,parentcategory)					
						cur.execute(query,data)	
						print "saving ",link,"\n"
						print articletext,"\n"
						insertcouter = insertcouter + 1
						conn.commit()
					else:
						print "not saving ",link,"\n"
						notsaved = notsaved +1
						f.write(link+"\n")
						iscrawled = False



				else:
					print "skipping ",link,"\n"
					skiped = skiped +1
			except Exception as e:
				print "error:  ",str(e),"\n"

		#print data,"\n"
		#print doc,"\n"	
		#print doc.category,"\n"

	
	f.write('insertcouter ='+str(insertcouter)+' notsaved =' +str(notsaved)+" skiped ="+str(skiped)+"\n")	
	f.close()

	print "legth of strin is "+str(len(articletext))+"\n"
	print insertcouter ," records inserted","\n"
	print "Operation done successfully";
conn.close()
