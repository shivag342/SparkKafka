import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

public final class JavaDirectKafkaWordCount  {
	static int i = 0;
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {
		/*
		 * if (args.length < 2) {
		 * System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
		 * "  <brokers> is a list of one or more Kafka brokers\n" +
		 * "  <topics> is a list of one or more kafka topics to consume from\n\n");
		 * System.exit(1); }
		 */
		Program p = new Program();
		String brokers = "localhost:2181";
		String topics = "test";

		// Create context with a 2 seconds batch interval
		SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount").setMaster("local");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("group.id", "test");

		// Create direct kafka stream with brokers and topics
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

		// Get the lines, split them into words, count the words and print
		JavaDStream<String> lines = messages.map(new Function<ConsumerRecord<String, String>, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			String jsonPrettyPrintString = "";

			@Override
			public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception {
				try {
					
					JSONObject xmlJSONObj = XML.toJSONObject(kafkaRecord.value());
					jsonPrettyPrintString = xmlJSONObj.toString();
					System.out.println(jsonPrettyPrintString);
					try {
						System.out.println("############ record number i = "+i);
						 p.getStartedDemo(i,jsonPrettyPrintString);
						System.out.println(String.format("###### Document creation complete with i="+i));
						i++;
					} catch (Exception e) {
						System.out.println(String.format("DocumentDB GetStarted failed with %s", e));
					}
				} catch (JSONException je) {
					System.out.println(je.toString());
				}
				return jsonPrettyPrintString;
			}
		});
		/*
		 * JavaDStream<String> words = lines.flatMap(x ->
		 * Arrays.asList(SPACE.split((CharSequence) x)).iterator());
		 * JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new
		 * Tuple2<>(s, 1)) .reduceByKey((i1, i2) -> i1 + i2); wordCounts.print(); //
		 * cosmosdb connection code // Gson gson=new Gson(); wordCounts.foreachRDD(rdd
		 * -> { System.out.printf("recode count : i =... %d ....\n", i); try { i++;
		 * //p.getStartedDemo(i++); System.out.println(String.
		 * format("Demo complete, please hold while resources are deleted")); } catch
		 * (Exception e) {
		 * System.out.println(String.format("DocumentDB GetStarted failed with %s", e));
		 * } });
		 */

		// end of cosmosdb connection code

		// Start the computation
		lines.print();
		jssc.start();
		jssc.awaitTermination();
	}

}
