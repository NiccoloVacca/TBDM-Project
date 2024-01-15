package flink_kafka_project_Maven;

import java.time.Instant;
import java.util.Date;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import com.google.gson.JsonObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
//This class is used for saving computed data from subreddit posts data inside the reddit-computed-data collection of tbdm-project database
public class MongoDBSinkComputed extends RichSinkFunction<JsonObject> {

	private transient MongoClient mongoClient;
    private transient MongoCollection<org.bson.Document> collection;

    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //Establish a connection to MongoDB upon opening the sink
        MongoClientURI uri = new MongoClientURI("mongodb://192.168.1.12:27017");
        mongoClient = new MongoClient(uri);
        MongoDatabase database = mongoClient.getDatabase("tbdm-project");
        collection = database.getCollection("reddit-computed-data");
    }

    @Override
    public void invoke(JsonObject value, Context context) throws Exception {
        //Check if the incoming 
    	if (value != null) {
    		//Create a MongoDB Document to store the data
        	 org.bson.Document document = new org.bson.Document();
        	 //Add the JsonObject to the document
             document.putAll(value.entrySet().stream()
                     .collect(java.util.stream.Collectors.toMap(
                             java.util.Map.Entry::getKey,
                             e -> e.getValue().isJsonPrimitive() ? e.getValue().getAsString() : e.getValue()
                     )));
             //Add the current timestamp to the document
             document.put("timestamp", Date.from(Instant.now()));
        	 //Insert the document into the MongoDB collection
             collection.insertOne(document);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        //Close the MongoDB client upon closing the sink
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

}
