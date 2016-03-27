from os import listdir
from os import getcwd
from bs4 import BeautifulSoup
print getcwd()
import json

filelist =[]
count =0
folderlist = ['entertainment','business','politics','sports','technology']
fs = open("bbcdataset.json","a")
folderpath = ''
jsonList = []
for folder in folderlist:
	folderpath = getcwd()+"/"+folder
	for f in listdir(folderpath):
		#if f.endswith(".txt"):
		filelist.append(f)
		#print folderpath
		#print f,"\n"

	
	for item in filelist:
		path = folderpath+"/"+item
		#print getcwd()+"/"+item,"\n"
		#soup = BeautifulSoup(open(path),'html.parser')
		
		article = open(path).read()
		label = folder
		#fs.write('{"text":"'+article+'",'+'"label":"'+label+'"}'+'\n')
		#print label,"\n"
		#print article,"\n"
		data = {
			"text" : article.decode('latin-1'),
			"label" : label.decode('latin-1')
		}
		jsondata = json.dumps(data)
		fs.write(jsondata + '\n')
		count = count  + 1
		
		#jsonList.append(data)
	filelist=[]
print count
#jsondata = json.dumps(data)
#fs.write(jsondata)
#jsondata = json.dumps(jsonList)
#fs.write(jsondata)
fs.close()

