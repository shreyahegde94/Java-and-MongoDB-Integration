/**
 * Implementing a simple program to demonstrate CRUD operations in Mongo DB using the Mongo Java Driver.
 */

package JavaMongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.Block;
import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

public class JavaMongoIntegration {

	/**
	 * In this function we are retrieving information from the MongoDB
	 *  and querying MongoDB to get desired information
	 */

	public static void RetrievingAndQuerying(MongoCollection<Document> collection) {
		
		System.out.println("The number of documents in the collection is " + collection.count());
		/**
		 * myFirstDoc contains the first document in your database
		 */
		Document myFirstDoc = collection.find().first();
		System.out.println(myFirstDoc.toJson());
		/**
		 * To retrieve all documents we need to chain find() method to iterator() method
		 */
		MongoCursor<Document> cursor = collection.find().iterator();
		try {
		    while (cursor.hasNext()) {
		        System.out.println(cursor.next().toJson());
		    }
		} 
		finally {
		    cursor.close();
		}
		/**
		 * To retrieve only documents that pass a certain condition
		 * pass a filter object to the find() method
		 * filter will contain the first document to have name as MongoDB
		 */
		Document filter = collection.find(eq("name","MongoDB")).first();
		System.out.println(filter.toJson());
		/**
		 * The following block of code will print all the documents where
		 * the field name is set to Java
		 */
		Block<Document> printBlock = new Block<Document>() {
		     @Override
		     public void apply(final Document document) {
		         System.out.println(document.toJson());
		     }
		};

		collection.find(eq("name", "Java")).forEach(printBlock);
		/**
		 * To print all the documents where the name is Integration and 
		 * the document_number is greater than 15 and less than or equal to 30
		 */
		collection.find(and(eq("name","Integration"),gt("document_number", 15), lte("document_number", 30))).forEach(printBlock);
		/**
		 * Retrieve document with document_number as 30
		 * and modify its name to modified
		 */
		collection.updateOne(eq("document_number", 30),set("name","modified"));
		Document modified = collection.find(eq("document_number",30)).first();
		System.out.println(modified.toJson());
		/**
		 * Updating multiple records
		 * Updating all records having a document number greater than 25
		 *  by adding a new field called additional_info
		 */
		UpdateResult updateResult = collection.updateMany(gt("document_number", 25), set("additional_info", "This document has a document_number greater than 25"));
		System.out.println(updateResult.getModifiedCount());
		collection.find(gt("document_number", 25)).forEach(printBlock);
		/**
		 * Deleting one or more documents from MongoDB
		 * Deletes the first document where name field is not MongoDB
		 * Deletes all the documents where name is Java
		 */
		collection.deleteOne(ne("name", "MongoDB"));
		DeleteResult deleteResult = collection.deleteMany(eq("name", "Java"));
		System.out.println(deleteResult.getDeletedCount());
		if(collection.find(eq("name","Java")) == null) {
			System.out.println("Delete operation was successful");
		}
		else {
			System.out.println("Delete operation was successful");

		}		
	}
	public static void main(String[] args) {
		
		/** 
		 * Creating an object "mongo" of class MongoClient 
		 * to connect to the local instance of MongoDB 
		 */
		
			MongoClient mongo = new MongoClient( "localhost" , 27017 );
			/**
			 * To connect to the database you need to specify the database name
			 * If database doesn't exist it automatically creates it
			 * To access the database you need to create a object of class DB
			 */
			MongoDatabase db = mongo.getDatabase("data");			
			/**
			 * if collection does not exist it automatically creates it
			 * or else it invokes the collection
			 */
			MongoCollection<Document> collection = db.getCollection("test");
			/**Inserting one document into collection "test"
			 * Each document comes with a unique identified called _id by default
			 * Randomly populating database with fields document_number,name and value
			 */
			 Document doc = new Document("document_number", 0)
			        .append("name", "database")
			        .append("value", (int)2*Math.random());
			collection.insertOne(doc);
			/**
			 * inserting multiple documents into MongoDB at once
			 */
			List<Document> documents = new ArrayList<Document>();
			String[] names = {"MongoDB","Java","Integration"};
			for (int i = 1; i <= 30; i++) {
				Document document1 = new Document();
				document1.put("document_number", i);
				document1.put("name", names[i%3]);
				document1.put("value",(int)i*Math.random());
			    documents.add(document1);
			}
			collection.insertMany(documents);
			JavaMongoIntegration.RetrievingAndQuerying(collection);
			
			
			mongo.close();			
	}

}
