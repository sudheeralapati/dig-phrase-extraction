package edu.isi.dig.prealignment;

import org.ahocorasick.trie.Token;
import org.ahocorasick.trie.Trie;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


public class PhraseExtractor {

	private static Logger LOG = Logger.getLogger(PhraseExtractor.class);
	

	public static void main(final String[] args) throws IOException, ClassNotFoundException, ParseException {
		String inputFile = args[0];
		String type = args[1];
		String keywordsFileName = args[2];
		String outputFile = args[3];
		Integer numOfPartitions = Integer.parseInt(args[4]);

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
		final String wordsListJson = new String(Files.readAllBytes(Paths.get(keywordsFileName)));


		JSONParser parser = new JSONParser();
		JSONObject keywordsJsonArray = (JSONObject) parser.parse(wordsListJson);

		final JSONObject misspellings = (JSONObject) keywordsJsonArray.get("misspellings");
		JSONArray wordsList = (JSONArray) keywordsJsonArray.get("wordsList");

		Map<String, Trie> triemap = new HashMap<>();
		createTries(wordsListJson,triemap);
	
		final Broadcast<Map<String, Trie>> broadcastedtriemap = sc.broadcast(triemap);
		final Broadcast<JSONArray> broadcastWordsList = sc.broadcast(wordsList);
		final Broadcast<org.json.simple.JSONObject> broadcastMisspellings = sc.broadcast(misspellings);

		if (type.equals("text")) {
			JavaRDD<String> jsonRDD = sc.textFile(inputFile,numOfPartitions);

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

					HashMap<String, HashSet<String>> keywordsMap = new HashMap<>();
					JSONArray wordsList = broadcastWordsList.getValue();

					for (int j = 0; j < wordsList.size(); j++) {
						HashSet<String> keywordsContainedList = new HashSet<>();
						if(rawText != null){
							JSONObject obj = (JSONObject) wordsList.get(j);
							Collection<Token> tokens = broadcastedtriemap.value().get(obj.get("name").toString()).tokenize(rawText);

							for (Token token : tokens) {
								if (token.isMatch()) {
									if(token.getFragment() != null)
										keywordsContainedList.add("\"" + token.getFragment() + "\"");
//									}
								}
							}
							keywordsMap.put((String) obj.get("name"),keywordsContainedList);
							row.put("wordslists", keywordsMap);
						}
					}

					return new Tuple2<>(new Text(line.split("\t")[0]), new Text(row.toJSONString()));


				}
			});
			wordsRDD.saveAsNewAPIHadoopFile(outputFile, Text.class, Text.class, SequenceFileOutputFormat.class);

		} else {
			JavaPairRDD<Text, Text> sequenceRDD = sc.sequenceFile(inputFile, Text.class, Text.class,numOfPartitions);
			
			
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
						if(rawText != null){
							JSONObject obj = (JSONObject) wordsList.get(j);
								Map<String, Trie> bTrieMap = broadcastedtriemap.value();
								if(bTrieMap != null && bTrieMap.get(obj.get("name").toString()) != null)
								{
									Collection<Token> tokens = bTrieMap.get(obj.get("name").toString()).tokenize(rawText);
									HashSet<String> keywordsContainedList = new HashSet<String>();
									for (Token token : tokens) {
										if (token.isMatch()) {
											if(token.getFragment() != null){
												String correct_word = (String) broadcastMisspellings.getValue().get(token.getFragment());
												if(correct_word != null)
													keywordsContainedList.add("\"" + correct_word + "\"");
												else
													keywordsContainedList.add("\"" + token.getFragment() + "\"");
											}
										}
									}
									keywordsMap.put((String) obj.get("name"),keywordsContainedList);
							}
						}
					}
				}			
				JSONObject jsonOutput = new JSONObject();
				jsonOutput.put("wordslists", keywordsMap);
				jsonOutput.put("uri",uri);
				
				return new Tuple2<>(new Text(uri), new Text(jsonOutput.toJSONString()));
			}
			
			});
			
			words = words.coalesce(numOfPartitions);
			words.saveAsNewAPIHadoopFile(outputFile, Text.class, Text.class, SequenceFileOutputFormat.class);
		}
	}
	
	private static void createTries(String wordsListJson,Map<String, Trie> triemap) throws ParseException{
		
		JSONParser parser = new JSONParser();
		JSONObject keywordsJsonArray = (JSONObject) parser.parse(wordsListJson);
		String capitalization = (String) keywordsJsonArray.get("capitalization");
		JSONArray wordsList = (JSONArray) keywordsJsonArray.get("wordsList");
		if (capitalization == null) {
			capitalization = "case insensitive";
		}
		for (int j = 0; j < wordsList.size(); j++) {
			Trie trie = null;
			if(capitalization.contains("insensitive")){
				trie = new Trie().removeOverlaps().onlyWholeWords().caseInsensitive();
			}else{
				trie = new Trie().removeOverlaps().onlyWholeWords();
			}
			JSONObject obj = (JSONObject) wordsList.get(j);
				ArrayList<String> wordList = (ArrayList<String>) obj.get("words");
				for (String word : wordList) {
					trie.addKeyword(word);
				}
			triemap.put(obj.get("name").toString(), trie);
		}
	}

}