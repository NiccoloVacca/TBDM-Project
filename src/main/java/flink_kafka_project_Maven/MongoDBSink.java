package flink_kafka_project_Maven;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

//This class is used for saving raw subreddit posts data inside the reddit-data collection of tbdm-project database
public class MongoDBSink extends RichSinkFunction<JsonObject> {

	private transient MongoClient mongoClient;
    private transient MongoCollection<org.bson.Document> collection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //Establish a connection to MongoDB upon opening the sink
        MongoClientURI uri = new MongoClientURI("mongodb://192.168.1.12:27017");
        mongoClient = new MongoClient(uri);
        MongoDatabase database = mongoClient.getDatabase("tbdm-project");
        collection = database.getCollection("reddit-data");
    }

    @Override
    public void invoke(JsonObject value, Context context) throws Exception {
        //Extract 'data' field from the incoming JsonObject (containing the data of the posts that we need)
    	JsonObject data = value.getAsJsonObject("data");
        if (data != null) {
        	JsonElement childrenElement = data.getAsJsonArray("children");
        	if (childrenElement != null && childrenElement.isJsonArray()) {
        	    JsonArray children = childrenElement.getAsJsonArray();

        	    if (children.size() > 0) {
        	        JsonObject postData = children.get(0).getAsJsonObject().getAsJsonObject("data");
        	        //Create a MongoDB document to store the data
        	        org.bson.Document document = new org.bson.Document();
        	        //Iterate over all the key-value pairs in the post data
        	        for (Map.Entry<String, JsonElement> entry : postData.entrySet()) {
        	            String key = entry.getKey();
        	            JsonElement jsonElement = entry.getValue();
        	            //Add the field to the document only if the value is not null
        	            if (!jsonElement.isJsonNull()) {
        	            	//In some cases the value is an array, so it has to be handled the proper way
        	                if (jsonElement.isJsonArray()) {
        	                    document.append(key, jsonElement.getAsJsonArray());
        	                } else {
        	                    document.append(key, jsonElement.isJsonObject() ? jsonElement.getAsJsonObject().toString() : jsonElement.getAsString());
        	                }
        	            }
        	        }
        	        //Insert the document into the MongoDB collection
        	        collection.insertOne(document);
        	    }
        	}
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
