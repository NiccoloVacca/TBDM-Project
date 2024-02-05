# TBDM-Project (Reddit Data Processing Application)
<a href="https://drive.google.com/file/d/1l7i4BsObaPFhirgYBhCYyYdp04ylP0fw/view?usp=sharing">Presentation of the Application</a>
## Table of Contents
1. Code Description
2. Pre-requisites
3. Installation
4. Configuration
5. Usage
6. Results Examples
## Code Description
This application is designed to fetch, process, and store data from Reddit posts published in the "gaming" subreddit. The workflow involves fetching data from Reddit using the Reddit API, streaming it to Kafka, applying real-time computation with Apache Flink, and storing both raw and computed results in MongoDB.
### First step - RedditDataFetcher Class
This class is a Java implementation designed to fetch data from the Reddit API. It uses libraries like: <b>Apache HttpClient</b> to make HTTP requests to Reddit’s API, <b>Gson</b> for JSON processing, including methods to authenticate, make requests and check the validity of access tokens (provided with Reddit development account).
The class starts by obtaining the OAuth token (through <i>“https://www.reddit.com/api/v1/access_token”</i> url) that will be used to make a request to the <b>Reddit API</b> (<i>“https://oauth.reddit.com/r/gaming/new.json?limit=1”</i>) and handling the API response.
#### Methods
* <b>'main'</b>:
  - Initiates Reddit API credentials (client ID, client secret, username, password)
  - Obtains an OAuth access token using the “getAccessToken” method
  - Validates the access token
  - If the access token is valid, it requests the latest post in the “gaming” subreddit using “makeRedditApiRequest” (only 1 new post each time)
* <b>'getAccessToken'</b>:
  - Constructs the Reddit API access token URL
  - Sends an HTTP Post request to the token URL with all the credentials (for getting the OAuth token)
  - Parses the JSON response to extract the access token
* <b>'makeRedditApiRequest'</b>:
  - Constructs an HTTP Get request for a specific Reddit API endpoint (for getting the latest post in a subreddit)
  - Sets User-Agent and Authorization headers using the obtained access token
  - Executes the request using Apache HttpClient
  - Processes and returns the response
* <b>'isAccessTokenValid'</b>:
  - Decode the access token to obtain information
  - Parses the decoded payload into a JsonObject using Gson
  - Extracts timestamp information (issued at, expires at) from the payload
  - Compares the expiration timestamp with the current time to determine token validity
### Second step - Producer Class
This Java class is part of the Flink application designed to fetch data from Reddit using the “<b>RedditDataFetcher</b>” class and stream it into a Kafka topic. It utilizes <b>Apache Flink</b> for stream processing and the Apache Kafka connector for Flink (“<b>FlinkKafkaProducer011</b>”). The main functionality includes configuring the Flink environment, defining a source function to produce Reddit data at regular intervals and setting up a Kafka producer to stream the data.
#### Methods
* <b>'main'</b>:
  - Logs the submission of the Flink job (using SLF4J library)
  - Specifies the Kafka output topic and server address
  - Invokes the “StreamProducer” method to initiate the data streaming process
* <b>'StreamProducer'</b>:
  - Retrieves the Flink execution environment
  - Adds a source from the StreamGenerator class, responsible for generating Reddit data
  - Configures and adds a Kafka sink to stream data to the specified output topic
  - Executes the Flink job
* <b>'StreamGenerator'</b>:
  - Implements the Flink “SourceFunction” interface to produce Reddit data
  - Runs a continuous loop fetching Reddit data using “RedditDataFetcher”
  - Filters out responses and collects valid Reddit Data (duplicated data is excluded)
  - Sleeps for 1 minute between iterations
* <b>'createStringProducer'</b>:
  - Creates a Flink Kafka producer for producing strings
### Third step - Consumer and Computation Class
This Java class is part of the Flink application designed to consume data from a Kafka topic, process the data using various **windowed operations**, and sink the results into **MongoDB**. It uses **Apache Flink** for stream processing, **Apache Kafka** for data ingestion and interacts with MongoDB for result storage.
#### Methods
* <b>'main'</b>:
  - Specifies the Kafka input topic (“reddit”) and server address
  - Invokes the “consumeFromKafka” method to start the data consumption processing
* <b>'consumeFromKafka'</b>:
  - Sets up the Flink environment and Kafka consumer with specified properties
  - Processes the incoming JSON data from Kafka (reddit posts data), filters out messages and applies various windowed operations
  - Prints and sinks the results into MongoDB using different sink functions
This class calls other classes within it:
* <b>'FilterDifferentIds'</b>: Discard reddit posts already fetched (checking the IDs)
* <b>'GlobalMostFrequentWordAggregate'</b>: Retrieve the **word that appears most frequently** in the “_title_” section of all the reddit posts read inside the time window
* <b>'Over18PercentageProcessFunction'</b>: Computes the **percentage of 18+ content** (“_over_18_” field) being true in all the reddit posts read inside the time window
* <b>'OriginalContentPercentageProcessFunction'</b>: Calculates the **percentage of original content** (“_is_original_content_” field) being true in all the reddit posts read inside the time window
* <b>'MostFrequentAuthorProcessFunction'</b>: Finds the **“_author_” that appears most frequently** in all the reddit posts read inside the time window
* <b>'MostFrequentDomainProcessFunction'</b>: Finds the **“_domain_” that appears most frequently** in all the reddit posts read inside the time window
* <b>'TimeDiffProcessFunction'</b>: Computes the **time difference between** the last “_created_utc_” (time of reddit post creation) read inside **the second-last time window and the first one**
* <b>'SubsDiffProcessFunction'</b>: Calculates the **difference between the last “_subreddit_subscribers_” read inside the time window and the first one**
This class also save both raw Reddit posts data and computed results in the MongoDB database (reddid-data collection and reddit-data-computed collection)
### Fourth step - MongoDB Manager Classes
2 classes are used for this purpose (MongoDBSink and MongoDBSinkComputed). Both of these Java classes are **Flink SinkFunctions** designed to write data into **MongoDB**, but they serve different purposes. Below is a comparison of the two:
#### Methods
* <b>'MongoDBSinkComputed' class</b> is intended to write computed data, such as the most frequently occurring word in “title”, the percentage of “is_original_content” being true, etc., into a collection named “_reddit-computed-data_” in the “tbdm-project” database
* <b>'MongoDBSink' class</b> is designed to write raw data from Reddit into a collection named “_reddit-data_” in the “tbdm-project” database
#### Key Points of the 2 MongoDB Manager Classes
* <b>Connection Opening</b>: Both classes open a connection to MongoDB in the “**open()**” method. They connect to the same MongoDB server but write to different collections
* <b>Connection Closing</b>: Both classes close the MongoDB connection in the “**close()**” method
* <b>Data Handling</b>:
  - “**MongoDBSinkComputed**” processes the incoming JsonObject directly, adding it to a MongoDB document. It also includes a timestamp of when it does this action
  - “**MongoDBSink**” extracts specific data (like the children array) from the JsonObject before constructing a MongoDB document
## Pre-requisites
Before running this application, ensure you have the following installed:
* Java (jdk-8 was used)
* Apache Flink (Flink 1.9.0 was used)
* Apache Kafka (Kafka 2.1.1 was used)
* MongoDB
## Installation
1. Clone this Repository
2. Install Java Development Kit <a href="https://www.oracle.com/it/java/technologies/javase/jdk11-archive-downloads.html">here</a>
3. Install Apache Flink <a href="https://flink.apache.org/downloads/#apache-flink-1163">here</a>
4. Install Apache Kafka <a href="https://kafka.apache.org/downloads">here</a>
5. Install MongoDB <a href="https://www.mongodb.com/docs/v3.0/administration/install-on-linux/">here</a>
6. Change Flink, Kafka and MongoDB server addresses; Change MongoDB details (DBs, collections); Optionally change subreddit name (now is r/gaming)
7. Build and Compile the Project using Maven command "_mvn clean install_" for getting a JAR file where the Main Class is Producer and a JAR where the Main Class is Consumer
8. Submit Producer JAR file into Flink platform
9. Submit Consumer JAR file into Flink platform
## Configuration
You will only have to change:
* Flink and Kafka server addresses at '<a href="https://github.com/NiccoloVacca/TBDM-Project/blob/main/src/main/java/flink_kafka_project_Maven/Consumer.java">_Consumer.Java_</a>' -> **row 33** AND '<a href="https://github.com/NiccoloVacca/TBDM-Project/blob/main/src/main/java/flink_kafka_project_Maven/Producer.java">_Producer.Java_</a>' -> **row 20**
* MongoDB server address at '<a href="https://github.com/NiccoloVacca/TBDM-Project/blob/main/src/main/java/flink_kafka_project_Maven/MongoDBSink.java">_MongoDBSink.java_</a>' -> **row 26** AND '<a href="https://github.com/NiccoloVacca/TBDM-Project/blob/main/src/main/java/flink_kafka_project_Maven/MongoDBSinkComputed.java">_MongoDBSinkComputed.java_</a>' -> **row 22**
* If you want to change the subreddit name, you can do it in '<a href="https://github.com/NiccoloVacca/TBDM-Project/blob/main/src/main/java/flink_kafka_project_Maven/RedditDataFetcher.java">_RedditDataFetcher_</a>' -> **row 40**
## Usage
These are the steps to run the Application, using the tools written in section "**Pre-Requisites**":
1. Ensure that the IP of the machine where Flink, Kafka and MongoDB are going to run is the correct one
2. Run Apache Flink by positioning in the Flink folder and typing '_./bin/start-cluster.sh_'
3. Run Zookeeper by positioning in the Kafka folder and typing '_bin/zookeeper-server-start.sh config/zookeeper.properties_'
4. Run Kafka with '_bin/kafka-server-start.sh config/server.properties_' command
5. Create a topic called '**reddit**' with the command: '_bin/kafka-topics.sh --create --topic reddit --bootstrap-server {machine_ip}:9092_' - If you want to change the name of the topic, remember to replace it also in the code
6. If it doesn't exist, create a database named '**tbdm-project**' with MongoDB and 2 Collections in this DB named '**reddit-data**' and '**reddit-computed-data**' - If you want to change the name of the DB and the Collections, remember to replace them also in the code
7. Run the first Flink job that will produce data from Reddit r/gaming subreddit just by producing the JAR file out of '**Producer.java**' class.
8. Run the second Flink job that will read, process and store data from Reddit r/gaming subreddit just by producing the JAR file out of '**Consumer.java**' class.
9. You can check the results in your MongoDB through MongoDB Compass or CLI
## Results Example
![raw_1](https://i.ibb.co/7Y21Gfx/reddit-data-screenshot1.jpg)
![raw_2](https://i.ibb.co/6wttKw1/reddit-data-screenshot2.jpg)
![computed](https://i.ibb.co/qpvW89F/reddit-data-computed-screenshot.jpg)
