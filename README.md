This Phrase Extractor uses aha-corasick algorithm(https://github.com/robert-bor/aho-corasick) to extract the keywords listed in wordList_preprocessed.json from ads json.

Step to run:

Step 1: wordList_preprocess.py script in /scripts directory combines all the keyword files present in https://github.com/usc-isi-i2/dig-alignment/tree/master/versions/2.0/wordlists/weapons to single json array and puts in wordList_preprocessed.json file

Step 2: Put the generated wordlist in this repository and generate a shaded jar using command "mvn package -P shaded"

Step 3: You can run a workflow in oozie as this is spark job, upload the jar having all the files and mention main class as PhraseExtractor

Arguments: 1) input json file <br />
2) type : json or seq <br />
3) keywords file : wordist_preprocessed.json <br />
4) outputfile <br />
<br /> 
How to run github wordlist script <br /> 
It pulls words from this link <br />
https://github.com/usc-isi-i2/dig-alignment/tree/development/versions/2.0/wordlists/weapons <br />

python wordList_preprocess.py --allowMisspellings [true/false] --output [pathtooutput]

