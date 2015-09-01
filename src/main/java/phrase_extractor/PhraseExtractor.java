package phrase_extractor;

import java.io.File;
import java.io.IOException;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import org.ahocorasick.trie.Token;
import org.ahocorasick.trie.Trie;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.CharSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import scala.Tuple2;


public class PhraseExtractor {

	private static Broadcast<Trie> bfT_Trie = null;
	private static Broadcast<Trie> bf_Trie = null;
	private static Broadcast<Trie> bg_Trie = null;
	private static Broadcast<Trie> bnfa_Trie = null;
	private static Broadcast<Trie> bnE_Trie = null;

	private static final String FIREARMS_TECHNOLOGY = "firearms-technology";
	private static final String FIREARMS = "firearms";
	private static final String GANG = "gang";
	private static final String NFA = "nfa";
	private static final String NON_ENGLISH = "non-english";

	public static void main(final String[] args) throws IOException, ClassNotFoundException, ParseException {
		String inputFile = args[0];
		String type = args[1];
		String keywordsFile = args[2];
		String outputFile = args[3];
		Integer numofPartitions = Integer.parseInt(args[4]);

		SparkConf conf = new SparkConf().setAppName("findKeywords").setMaster("local").registerKryoClasses(new Class<?>[]{
				Class.forName("org.apache.hadoop.io.LongWritable"),
				Class.forName("org.apache.hadoop.io.Text")
		});
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> keywordsRDD = sc.textFile(keywordsFile);
		final String wordsListJson = keywordsRDD.first();


		JSONParser parser = new JSONParser();
		JSONObject keywordsJsonArray = (JSONObject) parser.parse(wordsListJson);
		final Trie fTtrie = getTrie(wordsListJson,FIREARMS_TECHNOLOGY);
		final Trie ftrie = getTrie(wordsListJson,FIREARMS);
		final Trie gtrie = getTrie(wordsListJson,GANG);
		final Trie nfatrie = getTrie(wordsListJson,NFA);
		final Trie nEtrie = getTrie(wordsListJson,NON_ENGLISH);

		final JSONObject misspellings = (JSONObject) keywordsJsonArray.get("misspellings");
		JSONArray wordsList = (JSONArray) keywordsJsonArray.get("wordsList");

		bfT_Trie = sc.broadcast(fTtrie);
		bf_Trie = sc.broadcast(ftrie);
		bg_Trie = sc.broadcast(gtrie);
		bnfa_Trie = sc.broadcast(nfatrie);
		bnE_Trie = sc.broadcast(nEtrie);

		final Broadcast<JSONArray> broadcastWordsList = sc.broadcast(wordsList);
		final Broadcast<org.json.simple.JSONObject> broadcastMisspellings = sc.broadcast(misspellings);

		if (type.equals("text")) {
			JavaRDD<String> jsonRDD = sc.textFile(inputFile,numofPartitions);

			JavaPairRDD<Text, Text> wordsRDD= jsonRDD.mapToPair(new PairFunction<String, Text, Text>() {
				@Override
				public Tuple2<Text, Text> call(String line) throws Exception {

					JSONParser parser = new JSONParser();
					String doc = line.split("\t")[1];
					JSONObject row = (JSONObject) parser.parse(doc);
					String rawText = null;
					if(row.containsKey("text")){
						rawText = (String) row.get("text");
					}else if(row.containsKey("_source")){
						rawText = (String) ((JSONObject) row.get("_source")).get("raw_text");
					}

					HashMap<String, HashSet<String>> keywordsMap = new HashMap<String, HashSet<String>>();
					JSONArray wordsList = broadcastWordsList.getValue();

					for (int j = 0; j < wordsList.size(); j++) {
						HashSet<String> keywordsContainedList = new HashSet<String>();
						if(rawText != null){
							JSONObject obj = (JSONObject) wordsList.get(j);
							Collection<Token> tokens = getBroadcastTrie(obj.get("name").toString()).getValue().tokenize(rawText.toLowerCase());

							for (Token token : tokens) {
								if (token.isMatch()) {
									// takes the correct from misspellings mapping the json file
									// for getting the correct word
									//										String correct_word = (String) broadcastMisspellings.getValue().get(token.getFragment().toLowerCase());
									//										keywordsContainedList.add("\""+ correct_word + "\"");
									if(token.getFragment() != null)
										keywordsContainedList.add("\"" + token.getFragment() + "\"");
								}
							}
							keywordsMap.put((String) obj.get("name"),keywordsContainedList);
							row.put("wordslists", keywordsMap);
						}
					}

					return new Tuple2<Text, Text>(new Text(line.split("\t")[0]), new Text(row.toJSONString()));


				}
			});
			wordsRDD.saveAsNewAPIHadoopFile(outputFile, Text.class, Text.class, SequenceFileOutputFormat.class);

		} else {
			JavaPairRDD<Text, Text> sequenceRDD = sc.sequenceFile(inputFile, Text.class, Text.class);

			JavaPairRDD<Text, Text> words = sequenceRDD.flatMapValues(new Function<Text, Iterable<Text>>() {
				@Override
				public Iterable<Text> call(Text json) throws Exception {

					JSONParser parser = new JSONParser();
					JSONObject row = (JSONObject) parser.parse(json.toString());
					String rawText = null;
					if(row.containsKey("text")){
						rawText = (String) row.get("text");
					}else if(row.containsKey("_source")){
						rawText = (String) ((JSONObject) row.get("_source")).get("raw_text");
					}

					HashMap<String, HashSet<String>> keywordsMap = new HashMap<String, HashSet<String>>();
					JSONArray wordsList = broadcastWordsList.getValue();

					for (int j = 0; j < wordsList.size(); j++) {
						HashSet<String> keywordsContainedList = new HashSet<String>();
						if(rawText != null){
							JSONObject obj = (JSONObject) wordsList.get(j);
							Collection<Token> tokens = getBroadcastTrie(obj.get("name").toString()).getValue().tokenize(rawText.toLowerCase());

							for (Token token : tokens) {
								if (token.isMatch()) {
									// takes the correct from misspellings mapping the json file
//									String correct_word = (String) broadcastMisspellings.getV	alue().get(token.getFragment().toLowerCase());
//									keywordsContainedList.add("\""+ correct_word + "\"");
									if(token.getFragment() != null)
										keywordsContainedList.add("\"" + token.getFragment() + "\"");
								}
							}
							keywordsMap.put((String) obj.get("name"),keywordsContainedList);
							row.put("wordslists", keywordsMap);		
						}
					}

					return Arrays.asList(new Text(row.toJSONString()));
				}

			});
			//				words.saveAsTextFile(outputFile);
			words.saveAsNewAPIHadoopFile(outputFile, Text.class, Text.class, SequenceFileOutputFormat.class);
		}
	}


	private static Broadcast<Trie> getBroadcastTrie(String name){
		switch (name) {
		case FIREARMS_TECHNOLOGY:
			return bfT_Trie;
		case FIREARMS:
			return bf_Trie;
		case GANG:
			return bg_Trie;
		case NFA:
			return bnfa_Trie;
		case NON_ENGLISH:
			return bnE_Trie;
		default:
			break;
		}
		return null;
	}


	private static Trie getTrie(String wordsListJson,String type) throws ParseException{

		JSONParser parser = new JSONParser();
		JSONObject keywordsJsonArray = (JSONObject) parser.parse(wordsListJson);
		JSONArray wordsList = (JSONArray) keywordsJsonArray.get("wordsList");
		Trie trie = new Trie().removeOverlaps().onlyWholeWords().caseInsensitive();

		for (int j = 0; j < wordsList.size(); j++) {
			JSONObject obj = (JSONObject) wordsList.get(j);
			if (type.equals(obj.get("name").toString())) {
				ArrayList<String> wordList = (ArrayList<String>) obj.get("words");
				for (String word : wordList) {
					trie.addKeyword(word.toLowerCase());
				}
			}

		}

		return trie;

	}

}
