package phrase_extractor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.ahocorasick.trie.Token;
import org.ahocorasick.trie.Trie;
import org.apache.commons.logging.impl.Log4jFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import scala.Tuple2;


public class PhraseExtractor {

	private static Logger LOG = Logger.getLogger(PhraseExtractor.class);

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

		/*
		SparkConf conf = new SparkConf().setAppName("findKeywords").set("spark.io.compression.codec", "lzf").setMaster("local").registerKryoClasses(new Class<?>[]{
				Class.forName("org.apache.hadoop.io.LongWritable"),
				Class.forName("org.apache.hadoop.io.Text")
		});
		*/
		
		SparkConf conf = new SparkConf().setAppName("findKeywords").registerKryoClasses(new Class<?>[]{
				Class.forName("org.apache.hadoop.io.LongWritable"),
				Class.forName("org.apache.hadoop.io.Text")
		});
		
		
		@SuppressWarnings("resource")
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

		Map<String, Trie> triemap = new HashMap<String, Trie>();
		triemap.put(FIREARMS_TECHNOLOGY, fTtrie);
		triemap.put(FIREARMS, ftrie);
		triemap.put(GANG, gtrie);
		triemap.put(NFA, nfatrie);
		triemap.put(NON_ENGLISH, nEtrie);
	
		final Broadcast<Map<String, Trie>> broadcastedtriemap = sc.broadcast(triemap);
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
						rawText = (String) ((JSONObject) row.get("_source")).get("text");
					}

					HashMap<String, HashSet<String>> keywordsMap = new HashMap<String, HashSet<String>>();
					JSONArray wordsList = broadcastWordsList.getValue();

					for (int j = 0; j < wordsList.size(); j++) {
						HashSet<String> keywordsContainedList = new HashSet<String>();
						if(rawText != null){
							JSONObject obj = (JSONObject) wordsList.get(j);
							Collection<Token> tokens = broadcastedtriemap.value().get(obj.get("name").toString()).tokenize(rawText.toLowerCase());

							for (Token token : tokens) {
								if (token.isMatch()) {
//									if(allowMisspellings.toLowerCase().equals("true")){
//									 takes the correct from misspellings mapping the json file
//										String correct_word = (String) broadcastMisspellings.getValue().get(token.getFragment().toLowerCase());
//										keywordsContainedList.add("\""+ correct_word + "\"");
//									}else{
									if(token.getFragment() != null)
										keywordsContainedList.add("\"" + token.getFragment() + "\"");
//									}
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
			JavaPairRDD<Text, Text> sequenceRDD = sc.sequenceFile(inputFile, Text.class, Text.class,numofPartitions);
			
			
			JavaPairRDD<Text, Text> words = sequenceRDD.mapToPair(new PairFunction<Tuple2<Text,Text>, Text, Text>() {
			@Override
			public Tuple2<Text, Text> call(Tuple2<Text, Text> tuple) throws Exception {

				JSONParser parser = new JSONParser();
				JSONObject row = (JSONObject) parser.parse(tuple._2().toString());
				
				HashMap<String, HashSet<String>> keywordsMap = new HashMap<String, HashSet<String>>();
				String uri = ((String) row.get("uri"));
				
				if(row.containsKey("text")){
					String rawText = row.get("text").toString();
					
					JSONArray wordsList = broadcastWordsList.getValue();

					for (int j = 0; j < wordsList.size(); j++) {
						HashSet<String> keywordsContainedList = new HashSet<String>();
						if(rawText != null){
							JSONObject obj = (JSONObject) wordsList.get(j);
							if(obj != null && null != obj.get("name"))
							{
								Map<String, Trie> triemap = broadcastedtriemap.value();
								if(triemap != null && triemap.get(obj.get("name").toString()) != null)
								{
									Collection<Token> tokens = triemap.get(obj.get("name").toString()).tokenize(rawText.toLowerCase());
									
									for (Token token : tokens) {
										if (token.isMatch()) {
//											if(allowMisspellings.toLowerCase().equals("true")){
//											 takes the correct from misspellings mapping the json file
//												String correct_word = (String) broadcastMisspellings.getValue().get(token.getFragment().toLowerCase());
//												keywordsContainedList.add("\""+ correct_word + "\"");
//											}else{
											if(token.getFragment() != null)
												keywordsContainedList.add("\"" + token.getFragment() + "\"");
//											}
										}
									}
									keywordsMap.put((String) obj.get("name"),keywordsContainedList);
								}
							}
						}
					}
				}			
				JSONObject jsonOutput = new JSONObject();
				jsonOutput.put("wordslists", keywordsMap);
				jsonOutput.put("uri",uri);
				
				return new Tuple2<Text, Text>(new Text(uri), new Text(jsonOutput.toJSONString()));
			}
			
			});
			
//			words.saveAsTextFile(outputFile + new Random().nextInt());
//			words = words.coalesce(numofPartitions);
			words.saveAsNewAPIHadoopFile(outputFile, Text.class, Text.class, SequenceFileOutputFormat.class);
		}
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