package flink_kafka_project_Maven;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Producer {
	
	private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

	public static void main(String[] args) throws Exception {
		LOG.info("Job submitted");
		//Define Kafka output topic and server address
		String outputTopic = "reddit";
		String server = "192.168.1.12:9092";
		//Start the Flink job to produce data to Kafka
		StreamProducer(outputTopic, server);
	}
	
	public static void StreamProducer(String outputTopic, String server) throws Exception {
		//Set up the Flink execution environment
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		//Create a data stream from the class StreamGenerator (which read posts from reddit)
		DataStream<String> stringOutputStream = environment.addSource(new StreamGenerator());
		//Create a Kafka producer and connect it to the data stream
		FlinkKafkaProducer011<String> flinkKafkaProducer = createStringProducer(outputTopic, server);
		stringOutputStream.addSink(flinkKafkaProducer);
		//Command for executing Flink jobs
		environment.execute();
	}
	
	//This is the source function for generating data from Reddit (thanks to RedditDataFetcher) to the Kafka producer
	public static class StreamGenerator implements SourceFunction<String> {
		boolean flag = true;
		private static final long serialVersionUID = -999736771747691234L;
		public void run(SourceContext<String> ctx) throws Exception {
			while(flag) {
				//Fetching data here
				String redditData = RedditDataFetcher.main(null);
				
				// Check if response is not {"message":"Forbidden","error":403}
	            if (!redditData.equals("{\"message\":\"Forbidden\",\"error\":403}")) {
	                //Collect the data and send it to the downstream operator
	            	//The downstream operator is the FlinkKafkaProducer011 sink, that writes data in this context to the Kafka topic
	            	ctx.collect(redditData);
	            }
					
				//Wait for 5 minutes befor fetching again (usually a new post is created each 10 minutes in gaming subreddit)
				Thread.sleep(60000);
			}
			//Close the source context when the source is canceled
			ctx.close();
		}

		@Override
		public void cancel() {
			flag = false;
		}
	}
	
	//Class for creating the Kafka producer with a specified topic and Kafka address defined
	public static FlinkKafkaProducer011<String> createStringProducer(String topic, String kafkaAddress) {
		return new FlinkKafkaProducer011<>(kafkaAddress, topic, new SimpleStringSchema());
	}

}
