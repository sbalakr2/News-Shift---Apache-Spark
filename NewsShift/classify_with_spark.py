import string
import json 
import psycopg2
import os
from os import listdir
from os import getcwd

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer

from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes,NaiveBayesModel

# Module-level global variables for the `tokenize` function below
PUNCTUATION = set(string.punctuation)
STOPWORDS = set(stopwords.words('english'))
STEMMER = PorterStemmer()

# Function to break text into "tokens", lowercase them, remove punctuation and stopwords, and stem them
def tokenize(text):
    tokens = word_tokenize(text)
    lowercased = [t.lower() for t in tokens]
    no_punctuation = []
    for word in lowercased:
        punct_removed = ''.join([letter for letter in word if not letter in PUNCTUATION])
        no_punctuation.append(punct_removed)
    no_stopwords = [w for w in no_punctuation if not w in STOPWORDS]
    stemmed = [STEMMER.stem(w) for w in no_stopwords]
    result = [w for w in stemmed if w]
    if not result:
        return [""]
    return result

folderpath='hdfs://ec2-54-213-170-202.us-west-2.compute.amazonaws.com:9000/user/root/crawled_data'

sc = SparkContext()
data_raw = sc.wholeTextFiles(folderpath)
data_cleaned = data_raw.map(lambda (filename, text): (filename, tokenize(text)))
htf = HashingTF(50000)
data_hashed = data_cleaned.map(lambda (filename, text): (filename, htf.transform(text)))
data_hashed.persist()
sameModel = NaiveBayesModel.load(sc, 'hdfs://ec2-54-213-170-202.us-west-2.compute.amazonaws.com:9000/user/root/bbcmodel')
predictedLabel = data_hashed.map(lambda (filename, text): (filename.split("/")[-1][:-4],sameModel.predict(text)))
preds = predictedLabel.collect()

conn = psycopg2.connect(database="NewsSource", user="rakesh", password="davps2005", host="newdb.cnceaogjppz8.us-west-2.rds.amazonaws.com", port="5432")
cur = conn.cursor()
update = 'update articlestable set classifiedcategory=%s where id=%s'

newscategory = {hash('entertainment'):'entertainment',hash('sports'):'sports',hash('politics'):'politics',hash('technology'):'technology',hash('business'):'business'}

for pred in preds:       
      
    if(hash('entertainment')== pred[1]):
     category = 'entertainment'
    elif(hash('sports')== pred[1]):
     category = 'sports'
    elif(hash('politics')== pred[1]):
     category = 'politics'
    elif( hash('technology')== pred[1]):
     category = 'technology'
    elif(hash('business')== pred[1]):
     category = 'business'      
    
    data = (category, pred[0])

    cur.execute(update,data)
    conn.commit()
    print pred
