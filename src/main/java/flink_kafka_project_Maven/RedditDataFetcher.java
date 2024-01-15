package flink_kafka_project_Maven;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

public class RedditDataFetcher {

	public static String main(String[] args) throws Exception {
		String clientId = "aRyDlIzThX9lbsq2--Q2vw";
        String clientSecret = "35hgrVP6kMmuW8b2AF47mGlNhmqreg";
        String username = "niccolovacca21";
        String password = "nico1999";

        // Step 1: Get OAuth token
        String accessToken = getAccessToken(clientId, clientSecret, username, password);
        boolean isValid = isAccessTokenValid(accessToken);
        if(isValid) {
	        // Step 2: define and make a request to Reddit API using the obtained token (get 1 new post in subreddit {subreddit} at time)
	        String subreddit = "gaming";
	        String url = "https://oauth.reddit.com/r/" + subreddit + "/new.json?limit=1";
	        String response = makeRedditApiRequest(url, accessToken);
	
	        return response;
        } else {
        	return ("access token is not valid");
        }
	}
	
	private static String getAccessToken(String clientId, String clientSecret, String username, String password) throws Exception {
		//url for taking OAuth Token
		String stringUrl = "https://www.reddit.com/api/v1/access_token";
		//instantiate a CredentialsProvider object and configure it according to our personal credentials
		CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(clientId, clientSecret));
		//instantiate HttpClient object for api call to reddit
		HttpClient httpclient = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build();
		HttpPost httppost = new HttpPost(stringUrl);
		List<NameValuePair> params = new ArrayList<NameValuePair>(3);
		params.add(new BasicNameValuePair("grant_type","password"));
		params.add(new BasicNameValuePair("username",username));
		params.add(new BasicNameValuePair("password",password));
		
		try {
	        httppost.setEntity(new UrlEncodedFormEntity(params));
	        httppost.setHeader("User-Agent", "/u/ user v1.0");
	        HttpResponse response = httpclient.execute(httppost);
	        String responseString = EntityUtils.toString(response.getEntity(), HTTP.UTF_8);
	        Gson gson = new Gson();
	        // Extract access token from respons and return it
	        JsonObject jsonResponse = gson.fromJson(responseString, JsonObject.class);
	        JsonElement accessTokenElement = jsonResponse.get("access_token");
	        if (accessTokenElement != null && accessTokenElement.isJsonPrimitive()) {
	            return accessTokenElement.getAsJsonPrimitive().getAsString();
	        } else {
	            return null;
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	        return null;
	    }
    }
	
	//for getting new post in {subreddit} subreddit
	private static String makeRedditApiRequest(String url, String accessToken) throws Exception {
        HttpClient client = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);

        // Set Authorization header with the obtained access token
        httpGet.addHeader("Authorization", "Bearer " + accessToken);

        HttpResponse response = client.execute(httpGet);
        // Process the response and extract the data
        String responseString = EntityUtils.toString(response.getEntity(), HTTP.UTF_8);
        
        return responseString;
    }
	
	//check if token is expired
	private static boolean isAccessTokenValid(String accessToken) {
        try {
            // Decode access token for retrieving info
            String[] tokenParts = accessToken.split("\\.");
            String payload = tokenParts[1];
            byte[] decodedPayload = java.util.Base64.getUrlDecoder().decode(payload);
            String payloadJson = new String(decodedPayload);

            // Gson for json object
            Gson gson = new Gson();
            JsonObject payloadObject = gson.fromJson(payloadJson, JsonObject.class);
            
            // Extract creation timestamp and expiration timestamp from JSON payload
            JsonPrimitive issuedAt = payloadObject.getAsJsonPrimitive("iat");
            JsonPrimitive expiresAt = payloadObject.getAsJsonPrimitive("exp");

            // get now timestamp
            long currentTimeSeconds = System.currentTimeMillis() / 1000;

            // check if expiration timestamp is before now timestamp (expired)
            return issuedAt != null && expiresAt != null && expiresAt.getAsLong() > currentTimeSeconds;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

}
