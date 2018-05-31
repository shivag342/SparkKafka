package com.shiva.cosmosdb;

import java.io.IOException;
import java.io.Serializable;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.RequestOptions;

public class CosmosDBConnect implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public DocumentClient client;

	public void getStartedDemo(int i, String jsonPrettyPrintString) throws DocumentClientException, IOException {
		this.client = new DocumentClient("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
				"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
				new ConnectionPolicy(), ConsistencyLevel.Session);
		String docname = "KSdoc" + i;
		// Gson gson = new GsonBuilder().serializeNulls().create();
		// String jsonString=gson.toJson(jsonPrettyPrintString);
		this.createInputMsgDocumentIfNotExists("kfDB", "kfCol", jsonPrettyPrintString, docname);
	}

	private void writeToConsoleAndPromptToContinue(String text) throws IOException {
		System.out.println(text);
		System.out.println("Press any key to continue ...");
		// System.in.read();
	}

	// new custom method to insert kafka messages
	private void createInputMsgDocumentIfNotExists(String databaseName, String collectionName, String jsonString,
			String docname) throws DocumentClientException, IOException {
		try {
			String documentLink = String.format("/dbs/%s/colls/%s/docs/%s", databaseName, collectionName, docname);
			this.client.readDocument(documentLink, new RequestOptions());
		} catch (DocumentClientException de) {
			if (de.getStatusCode() == 404) {
				String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
				Document documentDefinition = new Document(jsonString);

				this.client.createDocument(collectionLink, documentDefinition, new RequestOptions(), false);
				this.writeToConsoleAndPromptToContinue(String.format("Created Family %s", jsonString));
			} else {
				throw de;
			}
		}
	}

}
