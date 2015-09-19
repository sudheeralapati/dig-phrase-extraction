

import urllib2
import json
import sys
import re

def getHttpResponse(url):
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    return response.read()

def preprocessWordList(url):
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    wordsListFiles = json.loads(response.read())
    wordsJsonArray={}
    wordsJsonArray['misspellings']={}
    for wordListFile in wordsListFiles:
        if wordListFile['name'] == 'json-wordlist.txt':
            download_url = wordListFile['download_url']
            file_contents = getHttpResponse(download_url)
            getWords(file_contents,wordsJsonArray)
    file = open(outputFilename,'w')
    file.write(json.dumps(wordsJsonArray))
    file.close()

def getWords(file_contents,wordsJsonArray):
    phrasesFile = json.loads(file_contents)
    phraseList = phrasesFile["phraselist"]
    wordsJsonArray['capitalization'] = phrasesFile['capitalization']
    wordsJsonArray['wordsList']=[]
    for phrasesType in phraseList:
        wordsList=[]
        listName = phrasesType["name"]
        phrases = phrasesType["phrases"]
        options = phrasesType["options"]
        for phraseObj in phrases:
            wordsList.extend(list(helper(listName,wordsJsonArray,phraseObj,options)))
        jsonObj={}
        jsonObj['name']=listName
        jsonObj['words'] = wordsList
        wordsJsonArray['wordsList'].append(jsonObj)

def helper(listName,wordJsonArray,phraseObj,options):
    word = phraseObj["phrase"]
    wordsList = [word]
    if "variations" in phraseObj:
        variations = phraseObj["variations"]
        for variation in variations:
            wordJsonArray['misspellings'][variation]=word
    if 'variations' not in phraseObj:
        editDistance=options['editDistance']
        fuzzyMatchingLength=options['fuzzyMatchingMinimumLength']
        if editDistance == 0:
            wordsList.extend(word)
        else:
            if containsNumbers(word) == False and len(word) > fuzzyMatchingLength and listName != 'non-english':
                wordsList.extend(edits(wordJsonArray,word))
    return wordsList


def containsNumbers(word):
    return any(char.isdigit() for char in word)

def edits(wordJsonArray,word):
   alphabet = 'abcdefghijklmnopqrstuvwxyz'
   splits     = [(word[:i], word[i:]) for i in range(1,len(word))]
   deletes    = [a + b[1:] for a, b in splits if b]
   transposes = [a + b[1] + b[0] + b[2:] for a, b in splits if len(b)>1]
   replaces   = [a + c + b[1:] for a, b in splits for c in alphabet if b]
   inserts    = [a + c + b for a, b in splits for c in alphabet]

   edit_words = set(deletes + transposes + replaces + inserts)

   for edit_word in edit_words:
       wordJsonArray['misspellings'][edit_word]=word
   return set(deletes + transposes + replaces + inserts)

def parse_args():
    global allowMisspellings
    global outputFilename

    for arg_idx, arg in enumerate(sys.argv):
        if arg == "--help":
            usage()
            exit(1)
        if arg == "--output":
            outputFilename = sys.argv[arg_idx+1]
            continue

def usage():
    print "Usage wordList_preprocess.py [--output]"

def die():
    print "Please input the required parameters"
    usage()
    exit(1)

outputFilename = None
parse_args()
if outputFilename is None:
    die()

url = "https://api.github.com/repos/usc-isi-i2/dig-alignment/contents/versions/2.0/wordlists/weapons?ref=development"

preprocessWordList(url)
