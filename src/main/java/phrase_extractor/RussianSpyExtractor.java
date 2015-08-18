package phrase_extractor;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.ahocorasick.trie.Token;
import org.ahocorasick.trie.Trie;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;



public class RussianSpyExtractor {
	
	public static void main(final String[] args) throws IOException, ClassNotFoundException, ParseException {
		String inputFile = args[0];
		String keywordsFile = args[1];
		String outputFile = args[2];
		
		JSONParser parser = new JSONParser();
		JSONObject keywordsJsonArray = (JSONObject) parser.parse(new FileReader(keywordsFile));
		final Trie trie = getTrie(keywordsJsonArray);
		
		FileInputStream fis = new FileInputStream(new File(inputFile));
		 
		//Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
	 
		String line = null;
		
		File fout = new File(outputFile);
		FileOutputStream fos = new FileOutputStream(fout);
	 
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		while ((line = br.readLine()) != null) {
//			String rawText = line.split("\t")[1];
			Collection<Token> tokens = trie.tokenize(line);
			JSONObject keywordsObj = new JSONObject();
			ArrayList<String> keywordsPresent = new ArrayList<String>();
			for(Token token : tokens){
				if(token.isMatch()){
					keywordsPresent.add(token.getFragment());
				}
			}
			keywordsObj.put("keywords", keywordsPresent);
			bw.write(keywordsObj.toJSONString() + "\t" + line );
			bw.newLine();
			
		}
		bw.close();
	}
	
	
	
	
	private static Trie getTrie(JSONObject keywordsjson) throws ParseException{
		
		
		JSONArray wordsList = (JSONArray) keywordsjson.get("wordsList");
		Trie trie = new Trie().removeOverlaps().onlyWholeWords().caseInsensitive();
		
		for (int j = 0; j < wordsList.size(); j++) {
			
					trie.addKeyword((String) wordsList.get(j));
			
		}
		
		return trie;

	}
	

	// this extracts the keywords which are present in all the 5 files from the raw_text
	private static HashMap<String, HashSet<String>> extractKeywords(String rawText,String wordsListJson) throws ParseException{
		JSONParser parser = new JSONParser();
		JSONObject keywordsJsonArray = (JSONObject) parser.parse(wordsListJson);
		JSONArray wordsList = (JSONArray) keywordsJsonArray.get("wordsList");
		JSONObject misspellings = (JSONObject) keywordsJsonArray.get("misspellings");

		HashMap<String, HashSet<String>> keywordsMap = new HashMap<String, HashSet<String>>();
		for (int j = 0; j < wordsList.size(); j++) {

			Trie trie = new Trie().removeOverlaps().onlyWholeWords().caseInsensitive();
			HashSet<String> keywordsContainedList = new HashSet<String>();
			JSONObject obj = (JSONObject) wordsList.get(j);
			ArrayList<String> wordList = (ArrayList<String>) obj.get("words");
			for (String word : wordList) {
				trie.addKeyword(word.toLowerCase());
			}
			Collection<Token> tokens = trie.tokenize(rawText.toLowerCase());
			for (Token token : tokens) {
				if (token.isMatch()) {
					// takes the correct from misspellings mapping the json file
					String correct_word = (String) misspellings.get(token.getFragment().toLowerCase());
					keywordsContainedList.add("\""+correct_word+"\"");
				}
			}
			keywordsMap.put((String) obj.get("name"),keywordsContainedList);

		}

		return keywordsMap;
	}


	

}
