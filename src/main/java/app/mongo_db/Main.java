package app.mongo_db;

import com.mongodb.MongoException;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {

        //getAllData();
        //getFilterableData();
        //applyProjections();
        //insertDocument();
        //insertManyDocuments();
        //updateDocument();
        //updateManyDocument();
        //deleteOneDocument();
        //deleteManyDocument();
        aggregateDocument();
    }

    private static void aggregateDocument() {
        MongoDatabase mongoDatabase = DBConnection.getDBConnection();
        MongoCollection<Document> collection = mongoDatabase.getCollection("users");

        try {
            AggregateIterable<Document> aggregateIterable = collection.aggregate(
                    List.of(
                            Aggregates.match(Filters.gt("age", 20)),
                            Aggregates.group("$city", Accumulators.sum("user_count", 1))
                    )
            );

            for(Document document : aggregateIterable) {
                System.out.println(document.toJson());
            }

        } catch (MongoException exception) {
            System.err.println(exception);
        }
    }

    private static void deleteManyDocument() {
        MongoDatabase mongoDatabase = DBConnection.getDBConnection();
        MongoCollection<Document> collection = mongoDatabase.getCollection("users");

        Bson query = Filters.eq("name", "Entry through Java - second doc");

        try {
            DeleteResult deleteResult = collection.deleteMany(query);

            System.out.println("Deleted count :: " + deleteResult.getDeletedCount());

        } catch (MongoException exception) {
            System.err.println(exception);
        }
    }

    private static void deleteOneDocument() {
        MongoDatabase mongoDatabase = DBConnection.getDBConnection();
        MongoCollection<Document> collection = mongoDatabase.getCollection("users");

        Bson query = Filters.eq("name", "Entry through Java - second doc");

        try {
            DeleteResult deleteResult = collection.deleteOne(query);

            System.out.println("Deleted count :: " + deleteResult.getDeletedCount());

        } catch (MongoException exception) {
            System.err.println(exception);
        }

    }

    private static void updateManyDocument() {
        MongoDatabase mongoDatabase = DBConnection.getDBConnection();
        MongoCollection<Document> collection = mongoDatabase.getCollection("users");

        Bson query = Filters.eq("name", "Entry through Java");
        Bson update = Updates.combine(
                Updates.set("age", 26),
                Updates.set("city", "San Jose, United States"),
                Updates.addToSet("hobbies", "Playing Cricket"),
                Updates.currentTimestamp("lastUpdated")
        );

        UpdateOptions options = new UpdateOptions().upsert(true);

        UpdateResult result = collection.updateOne(query, update, options);

        System.out.println(result.getModifiedCount());
        System.out.println(result.getUpsertedId());
    }

    private static void updateDocument() {
        MongoDatabase mongoDatabase = DBConnection.getDBConnection();
        MongoCollection<Document> collection = mongoDatabase.getCollection("users");

        //Method - 1
        //collection.updateOne(Filters.eq("name", "Deborah Allen"), Updates.set("age", 24));

        Document query = new Document("name", "Deborah Allen 1");
        //Bson update = Updates.set("age", 26);
        Bson update = Updates.combine(
                        Updates.set("age", 26),
                        Updates.set("city", "San Jose, United States"),
                        Updates.addToSet("hobbies", "Playing Cricket")
                    );

        UpdateOptions options = new UpdateOptions().upsert(true);

        UpdateResult result = collection.updateOne(query, update, options);

        System.out.println(result.getModifiedCount());
        System.out.println(result.getUpsertedId());
    }

    private static void insertManyDocuments() {
        MongoDatabase mongoDatabase = DBConnection.getDBConnection();
        MongoCollection<Document> collection = mongoDatabase.getCollection("users");

        List<Document> documentList = new ArrayList<>();
        Document document = new Document();
        document.append("name", "Entry through Java");
        document.append("age", 43);
        document.append("city", "Mumbai");
        document.append("location", Arrays.asList(14.24, 89.67));
        document.append("hobbies", Arrays.asList("Running", "Dancing"));

        document.append("education", new Document("university", "Utah, United States")
                .append("start", "02-2015")
                .append("end", "02-2019")
                .append("degree", "BTech"));

        documentList.add(document);

        Document document2 = new Document();
        document2.append("name", "Entry through Java - second doc");
        document2.append("age", 43);
        document2.append("city", "Mumbai");
        document2.append("location", Arrays.asList(14.24, 89.67));
        document2.append("hobbies", Arrays.asList("Running", "Dancing"));

        document2.append("education", new Document("university", "Utah, United States")
                .append("start", "02-2015")
                .append("end", "02-2019")
                .append("degree", "BTech"));

        documentList.add(document2);

        collection.insertMany(documentList);
    }

    private static void insertDocument() {
        MongoDatabase mongoDatabase = DBConnection.getDBConnection();
        MongoCollection<Document> collection = mongoDatabase.getCollection("users");

        Document document = new Document();
        document.append("name", "Entry through Java");
        document.append("age", 43);
        document.append("city", "Mumbai");
        document.append("location", Arrays.asList(14.24, 89.67));
        document.append("hobbies", Arrays.asList("Running", "Dancing"));

        document.append("education", new Document("university", "Utah, United States")
                .append("start", "02-2015")
                .append("end", "02-2019")
                .append("degree", "BTech"));

        collection.insertOne(document);
    }

    private static void applyProjections() {
        MongoDatabase mongoDatabase = DBConnection.getDBConnection();

        MongoCollection<Document> collection = mongoDatabase.getCollection("users");

        Bson projectionFields = Projections.fields(
                Projections.include("name", "age"),
                Projections.excludeId()
        );

        FindIterable<Document> iterable = collection.find(Filters.gt("age", 23))
                .projection(projectionFields)
                .sort(Sorts.descending("age"));

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