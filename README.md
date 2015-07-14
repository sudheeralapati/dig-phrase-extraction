# dig-phrase-extraction

This Phrase Extractor uses aha-corasick algorithm(https://github.com/robert-bor/aho-corasick) to extract the keywords listed in wordList_preprocessed.json from ads json.

Step to run:

Step 1: wordList_preprocess.py script in /scripts directory combines all the keyword files present in https://github.com/usc-isi-i2/dig-alignment/tree/master/versions/2.0/wordlists/weapons to single json array and puts in wordList_preprocessed.json file

Step 2: You can run a workflow in oozie as this is spark job, upload the jar having all the files and mention main class as PhraseExtractor

Arguments:
1) input json file 
2) type : json or seq
3) keywords file : wordist_preprocessed.json
4) outputfile 
=======
# dig-phrase-extraction
Code to extract phrases from text using the Aho-Corasick algorithm
>>>>>>> b41ff980c6bef02d5991fbf26f484aefe89db424
