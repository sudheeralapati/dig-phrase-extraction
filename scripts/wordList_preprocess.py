

import urllib2
import json
import sys
import re

def getHttpResponse(url):
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    return response.read()

def preprocessWordList(url,allowMisspellings):
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    wordsListFiles = json.loads(response.read())
    wordsJsonArray={}
    wordsJsonArray['wordsList']=[]
    wordsJsonArray['misspellings']={}
    for wordListFile in wordsListFiles:
        download_url = wordListFile['download_url']
        name = wordListFile['name']
        name = name[:-len('-wordlist.txt')]
        file_contents = getHttpResponse(download_url)
        wordsJsonArray['wordsList'].append(getWords(name,file_contents,wordsJsonArray,allowMisspellings,True))
    file = open(outputFilename,'w')
    file.write(json.dumps(wordsJsonArray))
    file.close()

def getWords(name,file_contents,wordsJsonArray,allowMisspellings,exactMatchForNonEnglish):
    words=file_contents.split("\n")
    #words = [word.lower() for word in words]
    misspelled_words=[]
    if allowMisspellings.lower() == 'true':
        for word in words:
            if containsNumbers(word)  == False and len(word) > 3 and (exactMatchForNonEnglish if name == 'non-english' else True):
                misspelled_words = misspelled_words + list(edits1(wordsJsonArray,word))
            wordsJsonArray['misspellings'][word.lower()]=word
    words_json={}
    words_json['name']=name
    words_json['words']=words + misspelled_words
    return words_json

def containsNumbers(word):
    return any(char.isdigit() for char in word)

def edits1(wordsJsonArray,word):
   alphabet = 'abcdefghijklmnopqrstuvwxyz'
   splits     = [(word[:i], word[i:]) for i in range(len(word) + 1)]
   deletes    = [a + b[1:] for a, b in splits if b]
   transposes = [a + b[1] + b[0] + b[2:] for a, b in splits if len(b)>1]
   replaces   = [a + c + b[1:] for a, b in splits for c in alphabet if b]
   inserts    = [a + c + b     for a, b in splits for c in alphabet]

   edit_words = set(deletes + transposes + replaces + inserts)

   for edit_word in edit_words:
       wordsJsonArray['misspellings'][edit_word.lower()]=word

   return set(deletes + transposes + replaces + inserts)

def parse_args():
    global allowMisspellings
    global outputFilename

    for arg_idx, arg in enumerate(sys.argv):
        if arg == "--help":
            usage()
            exit(1)
        if arg == "--allowMisspellings":
            allowMisspellings = sys.argv[arg_idx+1]
            continue
        if arg == "--output":
            outputFilename = sys.argv[arg_idx+1]
            continue

def usage():
    print "Usage wordList_preprocess.py [--allowMisspellings] [--output]"

def die():
    print "Please input the required parameters"
    usage()
    exit(1)

outputFilename = None
allowMisspellings = False
exactMatchNonenglishWords = True
parse_args()
if outputFilename is None:
    die()

url = "https://api.github.com/repos/usc-isi-i2/dig-alignment/contents/versions/2.0/wordlists/weapons?ref=development"

preprocessWordList(url,allowMisspellings)
