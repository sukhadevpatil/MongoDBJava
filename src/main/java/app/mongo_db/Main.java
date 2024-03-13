package app.mongo_db;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {

        //getAllData();
        //getFilterableData();
        applyProjections();

    }

    private static void applyProjections() {
        MongoDatabase mongoDatabase = DBConnection.getDBConnection();

        MongoCollection<Document> collection = mongoDatabase.getCollection("users");

        Bson projectionFields = Projections.fields(
                Projections.include("name", "age"),
                Projections.excludeId()
        );

        FindIterable<Document> iterable = collection.find(Filters.gt("age", 23))
                .projection(projectionFields).sort(Sorts.descending("age"));

        for(Document document : iterable) {
            System.out.println(document.toJson());
        }
    }

    private static void getAllData() {
        MongoDatabase mongoDatabase = DBConnection.getDBConnection();

        MongoCollection<Document> collection = mongoDatabase.getCollection("users");

        FindIterable<Document> iterable = collection.find();

        for(Document document : iterable) {
            System.out.println(document.toJson());
        }
    }

    private static void getFilterableData() {
        MongoDatabase mongoDatabase = DBConnection.getDBConnection();

        MongoCollection<Document> collection = mongoDatabase.getCollection("users");

        //FindIterable<Document> iterable = collection.find(Filters.eq("name", "Lauren Shaw"));
        FindIterable<Document> iterable = collection.find(Filters.gt("age", 30));

        for(Document document : iterable) {
            System.out.println(document.toJson());
        }
    }
}