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

