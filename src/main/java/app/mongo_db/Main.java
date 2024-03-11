package app.mongo_db;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        String uri = "mongodb://localhost:27017";

        MongoClient mongoClient = MongoClients.create(uri);

        MongoDatabase mongoDatabase = mongoClient.getDatabase("jobapp");

        MongoCollection<Document> collection = mongoDatabase.getCollection("users");

        //FindIterable<Document> iterable = collection.find();
        //FindIterable<Document> iterable = collection.find(Filters.eq("name", "Lauren Shaw"));
        FindIterable<Document> iterable = collection.find(Filters.gt("age", 30));

        for(Document document : iterable) {
            System.out.println(document.toJson());
        }
    }
}