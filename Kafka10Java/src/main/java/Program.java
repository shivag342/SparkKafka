
import java.io.IOException;
import java.io.Serializable;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DataType;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.Index;
import com.microsoft.azure.documentdb.IndexingPolicy;
import com.microsoft.azure.documentdb.RangeIndex;
import com.microsoft.azure.documentdb.RequestOptions;

public class Program implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public DocumentClient client;

	/**
	 * Run a Hello DocumentDB console application.
	 * 
	 * @param args
	 *            command line arguments
	 * @throws DocumentClientException
	 *             exception
	 * @throws IOException
	 */
	/*
	 * public static void main(String[] args) {
	 * 
	 * try { Program p = new Program(); p.getStartedDemo();
	 * System.out.println(String.
	 * format("Demo complete, please hold while resources are deleted")); } catch
	 * (Exception e) {
	 * System.out.println(String.format("DocumentDB GetStarted failed with %s", e));
	 * } }
	 */
	public void getStartedDemo(int i, String jsonPrettyPrintString) throws DocumentClientException, IOException {
		this.client = new DocumentClient("https://sparkkafka342.documents.azure.com:443/",
				"SkIRhYPmxzdhoRrryo5PMZyQ2WqlTsWYQvbNCCTHyidFb0shnNeQbYr65SUv9qxB9CH3oGlXBdvJFdrTtjcp4w==",
				new ConnectionPolicy(), ConsistencyLevel.Session);
		String docname="KSdoc"+i;
		//Gson gson = new GsonBuilder().serializeNulls().create();
		//String jsonString=gson.toJson(jsonPrettyPrintString);
		this.createInputMsgDocumentIfNotExists("kfDB", "kfCol", jsonPrettyPrintString,docname);
	}

/*	public void getStartedDemo(int i) throws DocumentClientException, IOException {
		this.client = new DocumentClient("https://sparkkafka342.documents.azure.com:443/",
				"SkIRhYPmxzdhoRrryo5PMZyQ2WqlTsWYQvbNCCTHyidFb0shnNeQbYr65SUv9qxB9CH3oGlXBdvJFdrTtjcp4w==",
				new ConnectionPolicy(), ConsistencyLevel.Session);
		Family andersenFamily = getAndersenFamilyDocument(i);
		this.createFamilyDocumentIfNotExists("kfDB", "kfCol", andersenFamily);
		// this.createInputMsgDocumentIfNotExists("kfDB","kfCol",string);

		
		 * this.createDatabaseIfNotExists("FamilyDB");
		 * this.createDocumentCollectionIfNotExists("FamilyDB", "FamilyCollection");
		 * 
		 * Family andersenFamily = getAndersenFamilyDocument();
		 * this.createFamilyDocumentIfNotExists("FamilyDB", "FamilyCollection",
		 * andersenFamily);
		 * 
		 * Family wakefieldFamily = getWakefieldFamilyDocument();
		 * this.createFamilyDocumentIfNotExists("FamilyDB", "FamilyCollection",
		 * wakefieldFamily);
		 * 
		 * this.executeSimpleQuery("FamilyDB", "FamilyCollection");
		 * 
		 * this.client.deleteDatabase("/dbs/FamilyDB", null);
		 
	}*/

	private Family getAndersenFamilyDocument(int i) {
		Family andersenFamily = new Family();
		andersenFamily.setId("Andersen.1" + i);
		andersenFamily.setLastName("Andersen");

		Parent parent1 = new Parent();
		parent1.setFirstName("Thomas");

		Parent parent2 = new Parent();
		parent2.setFirstName("Mary Kay");

		andersenFamily.setParents(new Parent[] { parent1, parent2 });

		Child child1 = new Child();
		child1.setFirstName("Henriette Thaulow");
		child1.setGender("female");
		child1.setGrade(5);

		Pet pet1 = new Pet();
		pet1.setGivenName("Fluffy");

		child1.setPets(new Pet[] { pet1 });

		andersenFamily.setDistrict("WA5");
		Address address = new Address();
		address.setCity("Seattle");
		address.setCounty("King");
		address.setState("WA");

		andersenFamily.setAddress(address);
		andersenFamily.setRegistered(true);

		return andersenFamily;
	}

	private Family getWakefieldFamilyDocument() {
		Family wakefieldFamily = new Family();
		wakefieldFamily.setId("Wakefield.7");
		wakefieldFamily.setLastName("Wakefield");

		Parent parent1 = new Parent();
		parent1.setFamilyName("Wakefield");
		parent1.setFirstName("Robin");

		Parent parent2 = new Parent();
		parent2.setFamilyName("Miller");
		parent2.setFirstName("Ben");

		wakefieldFamily.setParents(new Parent[] { parent1, parent2 });

		Child child1 = new Child();
		child1.setFirstName("Jesse");
		child1.setFamilyName("Merriam");
		child1.setGrade(8);

		Pet pet1 = new Pet();
		pet1.setGivenName("Goofy");

		Pet pet2 = new Pet();
		pet2.setGivenName("Shadow");

		child1.setPets(new Pet[] { pet1, pet2 });

		Child child2 = new Child();
		child2.setFirstName("Lisa");
		child2.setFamilyName("Miller");
		child2.setGrade(1);
		child2.setGender("female");

		wakefieldFamily.setChildren(new Child[] { child1, child2 });

		Address address = new Address();
		address.setCity("NY");
		address.setCounty("Manhattan");
		address.setState("NY");

		wakefieldFamily.setAddress(address);
		wakefieldFamily.setDistrict("NY23");
		wakefieldFamily.setRegistered(true);
		return wakefieldFamily;
	}

	private void createDatabaseIfNotExists(String databaseName) throws DocumentClientException, IOException {
		String databaseLink = String.format("/dbs/%s", databaseName);

		// Check to verify a database with the id=FamilyDB does not exist
		try {
			this.client.readDatabase(databaseLink, null);
			this.writeToConsoleAndPromptToContinue(String.format("Found %s", databaseName));
		} catch (DocumentClientException de) {
			// If the database does not exist, create a new database
			if (de.getStatusCode() == 404) {
				Database database = new Database();
				database.setId(databaseName);

				this.client.createDatabase(database, null);
				this.writeToConsoleAndPromptToContinue(String.format("Created %s", databaseName));
			} else {
				throw de;
			}
		}
	}

	private void createDocumentCollectionIfNotExists(String databaseName, String collectionName)
			throws IOException, DocumentClientException {
		String databaseLink = String.format("/dbs/%s", databaseName);
		String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);

		try {
			this.client.readCollection(collectionLink, null);
			writeToConsoleAndPromptToContinue(String.format("Found %s", collectionName));
		} catch (DocumentClientException de) {
			// If the document collection does not exist, create a new
			// collection
			if (de.getStatusCode() == 404) {
				DocumentCollection collectionInfo = new DocumentCollection();
				collectionInfo.setId(collectionName);

				// Optionally, you can configure the indexing policy of a
				// collection. Here we configure collections for maximum query
				// flexibility including string range queries.
				RangeIndex index = new RangeIndex(DataType.String);
				index.setPrecision(-1);

				collectionInfo.setIndexingPolicy(new IndexingPolicy(new Index[] { index }));

				// DocumentDB collections can be reserved with throughput
				// specified in request units/second. 1 RU is a normalized
				// request equivalent to the read of a 1KB document. Here we
				// create a collection with 400 RU/s.
				RequestOptions requestOptions = new RequestOptions();
				requestOptions.setOfferThroughput(400);

				this.client.createCollection(databaseLink, collectionInfo, requestOptions);

				this.writeToConsoleAndPromptToContinue(String.format("Created %s", collectionName));
			} else {
				throw de;
			}
		}

	}

	private void createFamilyDocumentIfNotExists(String databaseName, String collectionName, Family family)
			throws DocumentClientException, IOException {
		try {
			String documentLink = String.format("/dbs/%s/colls/%s/docs/%s", databaseName, collectionName,
					family.getId());
			this.client.readDocument(documentLink, new RequestOptions());
		} catch (DocumentClientException de) {
			if (de.getStatusCode() == 404) {
				String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
				this.client.createDocument(collectionLink, family, new RequestOptions(), true);
				this.writeToConsoleAndPromptToContinue(String.format("Created Family %s", family.getId()));
			} else {
				throw de;
			}
		}
	}

	private void executeSimpleQuery(String databaseName, String collectionName) {
		// Set some common query options
		FeedOptions queryOptions = new FeedOptions();
		queryOptions.setPageSize(-1);
		queryOptions.setEnableCrossPartitionQuery(true);

		String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
		FeedResponse<Document> queryResults = this.client.queryDocuments(collectionLink,
				"SELECT * FROM Family WHERE Family.lastName = 'Andersen'", queryOptions);

		System.out.println("Running SQL query...");
		for (Document family : queryResults.getQueryIterable()) {
			System.out.println(String.format("\tRead %s", family));
		}
	}

	@SuppressWarnings("unused")
	private void replaceFamilyDocument(String databaseName, String collectionName, String familyName,
			Family updatedFamily) throws IOException, DocumentClientException {
		try {
			this.client.replaceDocument(
					String.format("/dbs/%s/colls/%s/docs/%s", databaseName, collectionName, updatedFamily.getId()),
					updatedFamily, null);
			writeToConsoleAndPromptToContinue(String.format("Replaced Family %s", updatedFamily.getId()));
		} catch (DocumentClientException de) {
			throw de;
		}
	}

	@SuppressWarnings("unused")
	private void deleteFamilyDocument(String databaseName, String collectionName, String documentName)
			throws IOException, DocumentClientException {
		try {
			this.client.deleteDocument(
					String.format("/dbs/%s/colls/%s/docs/%s", databaseName, collectionName, documentName), null);
			writeToConsoleAndPromptToContinue(String.format("Deleted Family %s", documentName));
		} catch (DocumentClientException de) {
			throw de;
		}
	}

	private void writeToConsoleAndPromptToContinue(String text) throws IOException {
		System.out.println(text);
		System.out.println("Press any key to continue ...");
		// System.in.read();
	}

	// new custom method to insert kafka messages
	private void createInputMsgDocumentIfNotExists(String databaseName, String collectionName, String jsonString, String docname)
			throws DocumentClientException, IOException {
		try {
			String documentLink = String.format("/dbs/%s/colls/%s/docs/%s", databaseName, collectionName, docname);
			this.client.readDocument(documentLink, new RequestOptions());
		} catch (DocumentClientException de) {
			if (de.getStatusCode() == 404) {
				String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
				Document documentDefinition = new Document(jsonString) ;

				this.client.createDocument(collectionLink, documentDefinition, new RequestOptions(), false);
				this.writeToConsoleAndPromptToContinue(String.format("Created Family %s", jsonString));
			} else {
				throw de;
			}
		}
	}

	
}
