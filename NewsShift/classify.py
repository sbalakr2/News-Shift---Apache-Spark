import string
import json 

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
import hdfs

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
    return [w for w in stemmed if w]
	
# Initialize a SparkContext
sc = SparkContext()
folderpath='hdfs://ec2-54-213-170-202.us-west-2.compute.amazonaws.com/user/root/crawled_data'
newfolderpath='hdfs://ec2-54-213-170-202.us-west-2.compute.amazonaws.com/user/root/classified_data'

for f in listdir(folderpath):
	filelist.append(f)
	
for item in filelist:
	path=folderpath+"/"+item
	fi=open(path,'r')
	article=fi.read()
	fi.close()
	data_cleaned = tokenize(article)
	htf = HashingTF(50000)
	data_hashed =  htf.transform(data_cleaned)
	sameModel = NaiveBayesModel.load(sc, 'hdfs://ec2-54-213-170-202.us-west-2.compute.amazonaws.com/user/root/bbcmodel')
	prediction = sameModel.predict(data_hashed)
	classFile=open(newfolderpath+"/"+item,"w+")
	classFile.write(prediction)
	classFile.close()
	#print prediction
	os.remove(fi.name)
	sc.stop()

