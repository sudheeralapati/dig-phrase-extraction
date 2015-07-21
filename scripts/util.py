import json
import codecs
import re

#outputFile = open("output",'r')
outputFile=open('part-00000','r')
extractedKeys = codecs.open("prettyoutput.json",'w','utf-8')

jsonArray=[]
for line in outputFile:
    #line='{"wordlists" : ["handguns","pistols"]}'
    dict={}
    print line
    print line[22:-2]
    jsonString = json.loads(line[22:-2])
    raw_text = jsonString['_source']['raw_text']
    raw_text = re.sub(r"\s+", " ", raw_text, flags=re.UNICODE)
    wordslists = jsonString['wordslists']
    dict['raw_text']=raw_text
    dict['wordslists']=wordslists
    jsonArray.append(dict)

extractedKeys.write(json.dumps(jsonArray))



