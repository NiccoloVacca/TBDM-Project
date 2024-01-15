package flink_kafka_project_Maven;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.TypeHint;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class Consumer {

	public static void main(String[] args) throws Exception {
		//Define topic and kafka server from where it should read messages
		String inputTopic = "reddit";
        String server = "192.168.1.12:9092";
        //Function for retrieving data from kafka topic
        consumeFromKafka(inputTopic, server);
	}
	
	public static void consumeFromKafka(String inputTopic, String server) throws Exception {
        //Set up the execution environment for this job
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//Set properties for creating the Kafka consumer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", "flink-consumer-group");
        //Instantiate the Kafka consumer
        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>(inputTopic, new SimpleStringSchema(), properties);

        //Process data for each message received (with no time window)
        SingleOutputStreamOperator<JsonObject> processedDataStream = env.addSource(kafkaConsumer)
                .map(data -> {
                    try {
                    	//convert message do JsonObject
                        Gson gson = new Gson();
                        JsonObject jsonObject = gson.fromJson(data, JsonObject.class);
                        return jsonObject;
                    } catch (Exception e) {
                        System.err.println("Errore durante il parsing del JSON: " + e.getMessage());
                        return null;
                    }
                })
                //Take only the posts that we don't already have (checking by id in these reddit posts)
                .filter(jsonObject -> jsonObject != null && !isForbiddenMessage(jsonObject))
                .keyBy(new MessageKeySelector())
                .filter(new FilterDifferentIds());
        //Print data to console and add sink for writing data in MongoDB
        processedDataStream.print();
        processedDataStream.addSink(new MongoDBSink());
        //From now all the aggregation are done using time windows of 3500 seconds (computation will be applied each 3500 seconds)
        //Find word with more occurrencies in reddit posts titles
        SingleOutputStreamOperator<JsonObject> globalMetricsDataStream = processedDataStream
                .keyBy(new GlobalKeySelector())
                .timeWindow(Time.seconds(3500))
                .aggregate(new GlobalMostFrequentWordAggregate());
        globalMetricsDataStream.print();
        globalMetricsDataStream.addSink(new MongoDBSinkComputed());   
        //Find total percentage of over18 content
        SingleOutputStreamOperator<JsonObject> over18PercentageDataStream = processedDataStream
        	    .keyBy(new GlobalKeySelector())
        	    .timeWindow(Time.seconds(3500))
        	    .process(new Over18PercentageProcessFunction());
        over18PercentageDataStream.print();
        over18PercentageDataStream.addSink(new MongoDBSinkComputed());
        //Find total percentage of original content
        SingleOutputStreamOperator<JsonObject> originalContentPercentageDataStream = processedDataStream
        	    .keyBy(new GlobalKeySelector())
        	    .timeWindow(Time.seconds(3500))
        	    .process(new OriginalContentPercentageProcessFunction());
        originalContentPercentageDataStream.print();
        originalContentPercentageDataStream.addSink(new MongoDBSinkComputed()); 
        //Find the author that wrote more posts
        SingleOutputStreamOperator<JsonObject> mostFrequentAuthorDataStream = processedDataStream
        	    .keyBy(new GlobalKeySelector())
        	    .timeWindow(Time.seconds(3500))
        	    .process(new MostFrequentAuthorProcessFunction());
        mostFrequentAuthorDataStream.print();
        mostFrequentAuthorDataStream.addSink(new MongoDBSinkComputed());
        //Find the most used domain in reddit posts retrieved
        SingleOutputStreamOperator<JsonObject> mostFrequentDomainDataStream = processedDataStream
        	    .keyBy(new GlobalKeySelector())
        	    .timeWindow(Time.seconds(3500))
        	    .process(new MostFrequentDomainProcessFunction());
        mostFrequentDomainDataStream.print();
        mostFrequentDomainDataStream.addSink(new MongoDBSinkComputed());
        //Find time between last post and second-last post
        SingleOutputStreamOperator<JsonObject> timeLastPostDataStream = processedDataStream
        	    .keyBy(new GlobalKeySelector())
        	    .timeWindow(Time.seconds(3500))
        	    .process(new TimeDiffProcessFunction());
        	    
        timeLastPostDataStream.print();
        timeLastPostDataStream.addSink(new MongoDBSinkComputed());
        //Find number of subreddit subscribers difference from the start to the end of the time window
        SingleOutputStreamOperator<JsonObject> subsLastStreamDataStream = processedDataStream
        	    .keyBy(new GlobalKeySelector())
        	    .timeWindow(Time.seconds(3500))
        	    .process(new SubsDiffProcessFunction());
        subsLastStreamDataStream.print();
        subsLastStreamDataStream.addSink(new MongoDBSinkComputed());
        //Command for executing Kafka jobs
        env.execute("Kafka Consumer Job");
    }
	
	//Custom KeySelector to extract ID from JsonObject (reddit post content)
    public static class MessageKeySelector implements KeySelector<JsonObject, String> {
        @Override
        public String getKey(JsonObject value) {
            return value.get("data").getAsJsonObject().get("children").getAsJsonArray().get(0)
                    .getAsJsonObject().get("data").getAsJsonObject().get("id").getAsString();
        }
    }

    //Filter elements with different IDs compared to all the previous ones
    public static class FilterDifferentIds extends RichFilterFunction<JsonObject> {
        private transient ValueState<String> lastProcessedId;

        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("lastProcessedId", TypeInformation.of(new TypeHint<String>() {
            }));
            lastProcessedId = getRuntimeContext().getState(descriptor);
        }

        @Override
        public boolean filter(JsonObject current) throws Exception {
            String currentId = current.get("data").getAsJsonObject().get("children").getAsJsonArray().get(0)
                    .getAsJsonObject().get("data").getAsJsonObject().get("id").getAsString();
            String previousId = lastProcessedId.value();
            if (previousId != null && currentId.equals(previousId)) {
                //ID already exists, we discard the object
                return false;
            } else {
                //New ID, we keep it
                lastProcessedId.update(currentId);
                return true;
            }
        }
    }
    
    //Check if the message is an error, if so we can't process it
    public static boolean isForbiddenMessage(JsonObject json) {
        return json.has("message") && json.has("error") &&
                (json.get("error").getAsInt() == 403 || json.get("error").getAsInt() == 429);
    }
    
    //AggregateFunction for global most frequent word computation
    public static class GlobalMostFrequentWordAggregate implements AggregateFunction<JsonObject, Map<String, Integer>, JsonObject> {
        //private static final MostFrequentWordAggregate mostFrequentWordAggregate = new MostFrequentWordAggregate();

        public Map<String, Integer> createAccumulator() {
            return new HashMap<>();
        }
        //Extract the title
        public Map<String, Integer> add(JsonObject input, Map<String, Integer> accumulator) {
            String title = input.get("data").getAsJsonObject().get("children").getAsJsonArray().get(0)
                    .getAsJsonObject().get("data").getAsJsonObject().get("title").getAsString();

            //Get title as token
            String[] words = title.split("\\s+");

            // Update the accumulator with word frequencies
            for (String word : words) {
                accumulator.put(word, accumulator.getOrDefault(word, 0) + 1);
            }

            return accumulator;
        }

        public JsonObject getResult(Map<String, Integer> accumulator) {
            //Find the word with the highest frequency across all messages that we are processing
            String mostFrequentWord = null;
            int maxFrequency = 0;

            for (Map.Entry<String, Integer> entry : accumulator.entrySet()) {
                if (entry.getValue() > maxFrequency) {
                    mostFrequentWord = entry.getKey();
                    maxFrequency = entry.getValue();
                }
            }
            
            JsonObject result = new JsonObject();
            result.addProperty("most_popular_word_in_title", mostFrequentWord);
            //Emit the result as jsonobject with 1 single key,value couple
            return result;
        }

        public Map<String, Integer> merge(Map<String, Integer> a, Map<String, Integer> b) {
            //Merge two global accumulators to consolidate the word frequencies across parallel subtasks and create a unified global state
            for (Map.Entry<String, Integer> entry : b.entrySet()) {
                a.put(entry.getKey(), a.getOrDefault(entry.getKey(), 0) + entry.getValue());
            }
            return a;
        }
    }
    
    //KeySelector for global aggregation
    public static class GlobalKeySelector implements KeySelector<JsonObject, String> {
        @Override
        public String getKey(JsonObject value) throws Exception {
            return "global"; //Use a fixed key for all messages for global aggregation
        }
    }
    
    
    public static class Over18PercentageProcessFunction extends ProcessWindowFunction<JsonObject, JsonObject, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<JsonObject> elements, Collector<JsonObject> out) {
            int totalElements = 0;
            int trueOver18Count = 0;
            //For each element of the time window
            for (JsonObject element : elements) {
                totalElements++;
                //We get the value of the 'over_18' field in the reddit post
                JsonElement over18Element = element.get("data").getAsJsonObject().get("children").getAsJsonArray().get(0)
                        .getAsJsonObject().get("data").getAsJsonObject().get("over_18");
                //And we increase the counter of over_18, if content is +18
                if (over18Element != null && over18Element.isJsonPrimitive() && over18Element.getAsBoolean()) {
                	trueOver18Count++;
                }
            }
            //Then we calculate the total percentage of +18 content
            double over18Percentage = totalElements > 0 ? (trueOver18Count / (double) totalElements) * 100 : 0;

            JsonObject result = new JsonObject();
            result.addProperty("percentage_18plus", over18Percentage);

            out.collect(result);
        }
    }
    
    public static class OriginalContentPercentageProcessFunction extends ProcessWindowFunction<JsonObject, JsonObject, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<JsonObject> elements, Collector<JsonObject> out) {
            int totalElements = 0;
            int trueOriginalContentCount = 0;
            //For each element of the time window
            for (JsonObject element : elements) {
                totalElements++;
                //We get the value of 'is_original_content' in the reddit post
                JsonElement originalContentElement = element.get("data").getAsJsonObject().get("children").getAsJsonArray().get(0)
                        .getAsJsonObject().get("data").getAsJsonObject().get("is_original_content");
                //And if it's original we increase the counter for trueOgirinalContent
                if (originalContentElement != null && originalContentElement.isJsonPrimitive() && originalContentElement.getAsBoolean()) {
                	trueOriginalContentCount++;
                }
            }
            //Then we calculate the total percentage
            double originalContentPercentage = totalElements > 0 ? (trueOriginalContentCount / (double) totalElements) * 100 : 0;

            JsonObject result = new JsonObject();
            result.addProperty("percentage_original_content", originalContentPercentage);

            out.collect(result);
        }
    }
    
    public static class MostFrequentAuthorProcessFunction extends ProcessWindowFunction<JsonObject, JsonObject, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<JsonObject> elements, Collector<JsonObject> out) {
            Map<String, Integer> authorCountMap = new HashMap<>();
            //For each element of the time window
            for (JsonObject element : elements) {
            	//We get the author of the post
            	JsonElement authorElement = element.get("data").getAsJsonObject().get("children").getAsJsonArray().get(0)
                        .getAsJsonObject().get("data").getAsJsonObject().get("author");
            	//And we increase the counter for that specific author
            	if (authorElement != null && authorElement.isJsonPrimitive()) {
                    String author = authorElement.getAsString();
                    authorCountMap.put(author, authorCountMap.getOrDefault(author, 0) + 1);
                }
            }

            //Find the author with more occurrencies
            String mostFrequentAuthor = null;
            int maxCount = 0;

            for (Map.Entry<String, Integer> entry : authorCountMap.entrySet()) {
                if (entry.getValue() > maxCount) {
                    mostFrequentAuthor = entry.getKey();
                    maxCount = entry.getValue();
                }
            }

            JsonObject result = new JsonObject();
            result.addProperty("most_frequent_author", mostFrequentAuthor);

            out.collect(result);
        }
    }
    
    public static class MostFrequentDomainProcessFunction extends ProcessWindowFunction<JsonObject, JsonObject, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<JsonObject> elements, Collector<JsonObject> out) {
            Map<String, Integer> domainCountMap = new HashMap<>();
            //For each element of the time window
            for (JsonObject element : elements) {
            	//We get the domain of the reddit post
            	JsonElement domainElement = element.get("data").getAsJsonObject().get("children").getAsJsonArray().get(0)
                        .getAsJsonObject().get("data").getAsJsonObject().get("domain");
            	//And we increase the counter for that specific domain
            	if (domainElement != null && domainElement.isJsonPrimitive()) {
                    String author = domainElement.getAsString();
                    domainCountMap.put(author, domainCountMap.getOrDefault(author, 0) + 1);
                }
            }
            //Find the most frequent domain
            String mostFrequentDomain = null;
            int maxCount = 0;

            for (Map.Entry<String, Integer> entry : domainCountMap.entrySet()) {
                if (entry.getValue() > maxCount) {
                	mostFrequentDomain = entry.getKey();
                    maxCount = entry.getValue();
                }
            }

            JsonObject result = new JsonObject();
            result.addProperty("most_frequent_domain", mostFrequentDomain);

            out.collect(result);
        }
    }
    
    public static class TimeDiffProcessFunction extends ProcessWindowFunction<JsonObject, JsonObject, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<JsonObject> elements, Collector<JsonObject> out) {
            float largestTimestamp = 0;
            float secondLargestTimestamp = 0;
            long minutesDiffResult = 0;
          //For each element of the time window
            for (JsonObject element : elements) {
            	//We get the 'created_utc' value for the reddit post, which is timestamp in seconds
                JsonElement timestampElement = element
                        .get("data").getAsJsonObject()
                        .get("children").getAsJsonArray().get(0)
                        .getAsJsonObject().get("data").getAsJsonObject()
                        .get("created_utc");

                if (timestampElement != null && timestampElement.isJsonPrimitive()) {
                    float timestamp = timestampElement.getAsFloat();
                    //piece of code for getting the 2 biggest timestamps
                    if (timestamp > largestTimestamp) {
                        secondLargestTimestamp = largestTimestamp;
                        largestTimestamp = timestamp;
                    } else if (timestamp > secondLargestTimestamp) {
                        secondLargestTimestamp = timestamp;
                    }
                }
            }
            //Calculate the difference between the first and the second largest times (last and second-last posts) in seconds
            if (secondLargestTimestamp!=0) {
            	float diffResult = largestTimestamp - secondLargestTimestamp;
            	long longDiffResult = (long) diffResult * 1000L;
            	//Conversion in minutes
            	minutesDiffResult = longDiffResult / 60000L;
            }
            
            JsonObject result = new JsonObject();
			result.addProperty("minutes_since_last_post", minutesDiffResult);

            out.collect(result);
        }
    }
    
    public static class SubsDiffProcessFunction extends ProcessWindowFunction<JsonObject, JsonObject, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<JsonObject> elements, Collector<JsonObject> out) {
            int highestSubsNum = 0;
            int lowestSubsNum = 0;
            int subsDiffResult = 0;
            int count = 0;
            //For each element of the time window
            for (JsonObject element : elements) {
                //We get the number of 'subreddit_subscribers' at the moment of creation of the post
            	JsonElement subscribersElement = element
                        .get("data").getAsJsonObject()
                        .get("children").getAsJsonArray().get(0)
                        .getAsJsonObject().get("data").getAsJsonObject()
                        .get("subreddit_subscribers");
            	//And we get the highest number of subcribers and the lowest one.
            	//It usually corresponds to the last and first posts retrieved in the time window
                if (subscribersElement != null && subscribersElement.isJsonPrimitive()) {
                    int subscribers = subscribersElement.getAsInt();
                    if (count == 0) {
                    	lowestSubsNum = subscribers;
                    }
                    if (subscribers > highestSubsNum) {
                    	highestSubsNum = subscribers;
                    }
                    if (subscribers < lowestSubsNum) {
                    	lowestSubsNum = subscribers;
                    }
                }
                count++;
            }
            
            //Calculate the number of subs increased during the time window
            if (highestSubsNum!=0 && lowestSubsNum!=0) {
            	subsDiffResult = highestSubsNum - lowestSubsNum;
            }
            
            JsonObject result = new JsonObject();
			result.addProperty("subscribers_since_last_stream", subsDiffResult);

            out.collect(result);
        }
    }
    
    
}
