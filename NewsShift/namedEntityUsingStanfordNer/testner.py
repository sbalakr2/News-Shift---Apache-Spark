import psycopg2
import psycopg2
import urllib2
import urllib
#import urllib.parse
import xml.etree.ElementTree as etree
from bs4 import BeautifulSoup
import requests

from nltk.tag import StanfordNERTagger

def getWikiLinks(nelist):

	for entity in nelist:

		try:
			getVars = {'action': 'query', 'format': 'xml','titles':str(entity)}
			
			
			urlstring = 'https://en.wikipedia.org/w/api.php?'+str(entity)

			r = requests.get("https://en.wikipedia.org/w/api.php?", params=getVars)
			#response = urllib2.urlopen(urllib.parse.urlencode(urlstring))
			#print r.text
			print 
			response = r

		except Exception as e:
			print "error:  ",str(e),"\n"
			continue

		soup = BeautifulSoup(response.text,'xml')

		doc = soup.find('page')
		#print doc
		if(doc['_idx'] !='-1' ):
			print 'https://en.wikipedia.org/?curid='+doc['pageid']
			returnstr = 'https://en.wikipedia.org/?curid='+doc['pageid']
			returnlist =[returnstr,entity]
			return returnlist

def get_continuous_chunks(tagged_sent):
    continuous_chunk = []
    current_chunk = []

    for token, tag in tagged_sent:
        if tag != "O":
            current_chunk.append((token, tag))
        else:
            if current_chunk: # if the current chunk is not empty
                continuous_chunk.append(current_chunk)
                current_chunk = []
    # Flush the final current_chunk into the continuous_chunk, if any.
    if current_chunk:
        continuous_chunk.append(current_chunk)
    return continuous_chunk

#st = StanfordNERTagger('english.all.3class.distsim.crf.ser.gz','/root/trainingdata/stanfordner/stanford-ner.jar')
st = StanfordNERTagger('english.all.3class.distsim.crf.ser.gz','stanford-ner.jar')
#st = StanfordNERTagger('english.conll.4class.distsim.crf.ser.gz','stanford-ner.jar')


conn = psycopg2.connect(database="NewsSource", user="rakesh", password="davps2005", host="newdb.cnceaogjppz8.us-west-2.rds.amazonaws.com",port="5432")
cur = conn.cursor()
cur.execute("select articletext,id from articlestable where order by crawleddatetime desc")
#cur.execute("select articletext,id from articlestable where COALESCE(wikilinks,'') = '' order by crawleddatetime desc")
#cur.execute("select articletext,id from articlestable where order by crawleddatetime desc")
updateq = "update articlestable set wikilinks= %s where id=%s"
rows = cur.fetchall()
for row in rows:
	print row[0]
	print 
	py = st.tag(row[0].split())	
	ne_tagged_sent = py

	named_entities = get_continuous_chunks(ne_tagged_sent)
	named_entities = get_continuous_chunks(ne_tagged_sent)
	named_entities_str = [" ".join([token for token, tag in ne]) for ne in named_entities]
	named_entities_str_tag = [(" ".join([token for token, tag in ne]), ne[0][1]) for ne in named_entities]

	ner = named_entities_str_tag
	people =[]
	places =[]
	organ =[]

	print named_entities_str_tag
	print
	for entity in ner:
		if (entity[1] == 'PERSON'):
			people.append(entity[0].replace(',','').replace(';',''))
		elif (entity[1] == 'ORGANIZATION'):
			organ.append(entity[0].replace(',','').replace(';',''))
		elif (entity[1] == 'LOCATION'):
			places.append(entity[0].replace(',','').replace(';',''))


	try:
		wikilink =[]
		storelist =[]

		print people
		print
		if(len(people)>0):
			storelist = getWikiLinks(people)
			wikilink.append(storelist[0]+','+storelist[1])
			print wikilink
			print
			
		if(len(places)>0):
			storelist = getWikiLinks(places)
			wikilink.append(storelist[0]+','+storelist[1])
			print wikilink
			print
			
		if(len(organ)>0):
			storelist = getWikiLinks(organ)
			wikilink.append(storelist[0]+','+storelist[1])
			print wikilink
			print
	except Exception as e:
		print str(e)

	print str(wikilink).strip('[]')
	print 
	dataq = (str(wikilink).strip('[]'),row[1])
	cur.execute(updateq,dataq)
	conn.commit()















