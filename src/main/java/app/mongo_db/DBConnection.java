package app.mongo_db;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

public class DBConnection {

    public static MongoDatabase getDBConnection() {
        String uri = "mongodb://localhost:27017";

        MongoClient mongoClient = MongoClients.create(uri);

        return mongoClient.getDatabase("jobapp");
    }
}
