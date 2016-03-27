# testing
# coding: latin-1
import psycopg2
import time
from os import listdir
from os import getcwd
from bs4 import BeautifulSoup
import pandas as pd    
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem.porter import*
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import xml.etree.ElementTree as etree
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET
from sklearn.externals import joblib
import cPickle

stemmer = PorterStemmer()
def review_to_words(raw_review):
    review_text = BeautifulSoup(raw_review, "lxml").get_text()      
    letters_only = re.sub("[^a-zA-Z]", " ", review_text) 
    words = letters_only.lower().split()  
    stops = set(stopwords.words("english"))                  
    meaningful_words = [w for w in words if not w in stops]  
    final_words = [stemmer.stem(word) for word in meaningful_words] 
    return( " ".join(final_words))

forest1 = joblib.load('bbcmodel.pkl')

conn = psycopg2.connect(database="NewsSource", user="rakesh", password="davps2005", host="newdb.cnceaogjppz8.us-west-2.rds.amazonaws.com", port="5432")
cur = conn.cursor()

update = 'update articlestable set classifiedcategory=%s where id=%s'
#article = 'These undated photos show Michael Cavallari, brother of reality TV star Kristin Cavallari, and his Honda Civic, which was found off Instate 70 in Grand County, Utah Nov. 27, 2015. (Grand County Sheriffs Office)This 2012 photo shows reality star Kristin Cavallari and her now-husband, NFL quarterback Jay Cutler (Reuters)Authorities in Utah said Sunday that a man who has been missing for more than a week is the brother of former MTV reality star Kristin Cavallari and the brother-in-law of NFL quarterback Jay Cutler. The Grand County Sheriffs Office said it first became involved in the search for Michael Cavallari, 30, on Nov. 27, when it received a report of an abandoned Honda Civic along Interstate 70 in the southeastern part of the state. Authorities said Cavallari was identified as the cars driver after credit card receipts in the vehicle led them to a convenience store in Monticello, Utah, where they saw him in footage on the stores security system.The Deseret News reported that Cavallaris laptop and cellphone were found inside the car, which appeared to have hit a large rock on the side of the road. The vehicles engine was running and its airbag was deployed.Were combing the area for anything we can find, Grand County Sheriff Steven White told the paper. Were deeming it suspicious, but theres nothing to indicate anything one way or another. The vehicle was just abandoned.Michael Cavallari lives in San Clemente, California. Grand County is about 100 miles north of Monticello and 225 miles south of Salt Lake City.Michael Cavallaris younger sister, Kristin, rose to fame while still in high school after being cast in MTVs reality series Laguna Beach. The show aired for three seasons before ending in 2006. Cavallari also made several appearances on the shows spin-off series The Hills.Kristin Cavallari married Jay Cutler in November 2013. The couple have three children.The Associated Press contributed to this re'
while True:

	cur.execute("select id,articletext,parentcategory,category from articlestable where COALESCE(classifiedcategory,'') = '' order by crawleddatetime desc limit 100")
	rows = cur.fetchall()

	for row in rows:
		article = row[1]
		iden 	= row[0]

		if(len(article) > 150):
			clean_test_reviews = [] 
			clean_review = review_to_words(article)
			clean_test_reviews.append(clean_review)
			result = forest1.predict(clean_test_reviews)
			data = (result[0],iden)
			print result[0],"   actually is ",row[2],"\n"
		else:
			data = ('toosmall',iden)
			print "toosmall   actually is ",row[2],"\n"
		if(row[3]=='Sports'):
			data = ('Sports',iden)

		
		cur.execute(update,data)

		conn.commit()
	print "Pass complete"
	print "waiting for 4 seconds \n"	
	time.sleep(4)
	conn.commit()

