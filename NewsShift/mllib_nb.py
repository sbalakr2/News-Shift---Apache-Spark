import string
import json 

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer

from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes

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
# Import full dataset of newsgroup posts as text file
#data_raw = sc.textFile('hdfs://ec2-54-213-237-76.us-west-2.compute.amazonaws.com:9000/trainingdata/trainingdata/bbcjsontxt')
data_raw = sc.textFile('bbcdataset.json')

# Parse JSON entries in dataset
data = data_raw.map(lambda line: json.loads(line))
# Extract relevant fields in dataset -- category label and text content
data_pared = data.map(lambda line: (line['label'], line['text']))
# Temporary print statement for testing partial script
print data_pared.first()

# Prepare text for analysis using our tokenize function to clean it up
data_cleaned = data_pared.map(lambda (label, text): (label, tokenize(text)))

# Hashing term frequency vectorizer with 50k features
htf = HashingTF(50000)

# Create an RDD of LabeledPoints using category labels as labels and tokenized, hashed text as feature vectors
data_hashed = data_cleaned.map(lambda (label, text): LabeledPoint(hash(label), htf.transform(text)))

# Ask Spark to persist the RDD so it won't have to be re-created later
data_hashed.persist()
# Train a Naive Bayes model on the training data
model = NaiveBayes.train(data_hashed)

#model.save(sc, "hdfs://ec2-54-213-237-76.us-west-2.compute.amazonaws.com:9000/trainingdata/trainingdata/bbcmodela")
model.save(sc, "bbcmodel")
