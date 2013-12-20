package perf_eval;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;

import logger.LogSetup;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.InfrastructureMetadata;
import common.messages.InvalidMessageException;
import common.messages.ServerData;
import app_kvServer.KVServer;

public class Evaluator {
	
	// Keywords specify which lines we look for in a file from the enron dataset.
	// Most enron files have a Date and Subject tag. So we use the Date as the key and the Subject as the value.
	public static final String LINE_KEYWORD_KEY = "Date: ";
	public static final String LINE_KEYWORD_VALUE = "Subject: ";
	
	private int numClients;
	private int numKVPairs = 20000;
	private int numRequestsPerClient;
	private String enronPath = "";
	private String enronFileCachePath = "enronFiles.cache";
	
	private ArrayList<KVServer> servers;	// list of servers
	private ArrayList<ClientWrapper> clients;	// list of clients
	private ArrayList<Thread> clientThreads;
	private HashMap<ClientWrapper, HashMap<String, String>> requestMap; // contains clients and their corresponding request maps, client -> kvMap
	private HashMap<Integer, String> indexMap; // contains IDs and their keys, ID -> key
	private HashMap<String, String> kvMap;	// key value map with data from enron dataset, keyString -> valueString
	private HashMap<Integer, String> enronFiles; // map: enronfileIDs --> file path
	public Logger logger;
	
	public Evaluator(String enronPath, int numClients, int numDataPairs, int numRequestsPerClient) {
		this.enronPath = enronPath;
		this.numKVPairs = numDataPairs;
		this.numClients = numClients;
		this.numRequestsPerClient = numRequestsPerClient;
			
		LogSetup ls = new LogSetup("logs/eval.log", "PE", Level.ALL);
		this.logger = ls.getLogger();
		
		clients = new ArrayList<ClientWrapper>();
		kvMap = new HashMap<String, String>();
		indexMap = new HashMap<Integer, String>();
		requestMap = new HashMap<ClientWrapper, HashMap<String, String>>();
		enronFiles = new HashMap<Integer, String>();
		
		for (int i = 0; i < this.numClients; i++) {
			clients.add(new ClientWrapper("CLIENT " + i, this));
		}
	}
	
	public Logger getLogger() {
		return this.logger;
	}
	
	/**
	 * Build and read a cache of filepaths of enron dataset.
	 * @param enronPath path to the maildir directory of the enron dataset
	 * @throws IllegalArgumentException Thrown if the path is invalid or the cache file cannot be written/accessed. 
	 */
	private void getEnronFiles(String enronPath) throws IllegalArgumentException {
		File fileCache = new File(enronFileCachePath);
		
	    if (fileCache.exists() && !fileCache.isFile()) {
	        throw new IllegalArgumentException("The path enronFiles.cache is a directory. Please delete it and retry.");
	    }
	    
		if (!fileCache.exists()) { // build the enron file cache if it doesn't exist
			logger.error("The Enron filepath cache was not found at " + enronFileCachePath);
			
			try {
				fileCache.createNewFile();
			    if (!fileCache.canWrite()) {
			        throw new IllegalArgumentException("Unable to create filepath cache. File cannot be written: " + enronFileCachePath);
			    }
			    
			    logger.info("Obtaining Enron filepath data from provided path: " + enronPath + ". Please wait...");
			    
				File baseDir = new File(enronPath);
				Collection<File> enronFileList = FileUtils.listFiles(baseDir, null, true);
			
				logger.info("Building new filepath cache. Please Wait, this can take a while...");
				Writer output = new BufferedWriter(new FileWriter(fileCache));
			
				try {
					for (File file : enronFileList) {
				      //Write paths to file
				      output.write(file.getAbsolutePath() + "\n");
					}
				} finally {
				      output.close();
				}
			} catch (IOException ex) {
				logger.error("IOException while trying to create filepath cache.\nEnronPath: " + enronPath + "\nFileCachePath: " + enronFileCachePath + "\n" + ex.getMessage());
				ex.printStackTrace();
			}
		}
		
		
		// Get all paths from file cache
		try {
			BufferedReader enronCachedPaths = new BufferedReader(new FileReader(fileCache));
		
			logger.info("Reading filepaths from file cache. Please Wait, this can take a while...");
			try {
				String line = null;
				
				int i = 0;
				while ((line = enronCachedPaths.readLine()) != null) {
						enronFiles.put(i, line);
						i++;
				}
				
				logger.info("Enron filepaths initialized. There are " + enronFiles.size() + " files in the set.");
				
			} finally {
				enronCachedPaths.close();
			}
		} catch (IOException ex) {
			logger.error("IOException while trying to read filepath cache.\nEnronPath: " + enronPath + "\nFileCachePath: " + enronFileCachePath + "\n" + ex.getMessage());
			ex.printStackTrace();
		}
	}
	
	/**
	 * Initializes Enron files for processing and obtains the specified number of key-value data of specified format from the Enron dataset.
	 */
	public void initEnron() {
		
		try {
			getEnronFiles(enronPath); // initialize filepath cache
		} catch (IllegalArgumentException ex) {
			logger.error("Unable to initialize enron filepath cache. The program will now exit.");
			System.exit(1);
		}
	
		
		BufferedReader contents;
		
		logger.info("Processing Enron Dataset and generating " + numKVPairs + " data request pairs.");
		logger.info("Line keyword for keys: " + LINE_KEYWORD_KEY);
		logger.info("Line keyword for Values: " + LINE_KEYWORD_VALUE);
		logger.info("Please wait...  0 / " + numKVPairs);
		// Iterate over enron file paths
		for (int i = 0; i < enronFiles.size(); i++) {
			try {
				int numIndexed = kvMap.size();
				
				if (numIndexed >= numKVPairs)
					break; // If maximum number of KV Pairs in set, stop processing enron data here.
				
				
				
				
				if (numIndexed != 0 && (numIndexed % 10000) == 0) {
					logger.info("\rPlease wait...  " + numIndexed + " / " + numKVPairs);
				}
				
				// Obtain enron file contents for current file in set
				File enronFile = new File(enronFiles.get(i));
				contents = new BufferedReader (new FileReader(enronFile));
				
				try { // read the file and try to obtain the specified key->value data.
					String line = null;
					String valKey = "";
					String valValue = "";
					
					while ((line = contents.readLine()) != null) {
						if (line.startsWith(LINE_KEYWORD_KEY) && valKey.equals("")) {
							valKey = line.substring(LINE_KEYWORD_KEY.length());
						} else if (line.startsWith(LINE_KEYWORD_VALUE) && valValue.equals("")) {
							valValue = line.substring(LINE_KEYWORD_VALUE.length());
						}
						
						if (!valKey.equals("") && !valValue.equals(""))
							break; // If we found both key and value, stop reading lines from the file.
					}
					
					// If we obtained Key-Value data from the file, add it to the KVMap
					if (!valKey.equals("") && !valValue.equals("")) {
						if (!kvMap.containsKey(valKey)) {
							indexMap.put(kvMap.size(), valKey);
							kvMap.put(valKey, valValue);
							// System.out.println(kvMap.size());
						}
					}
				} finally {
					contents.close();
				}
			} catch (IOException ex) {
				logger.error("IOException occured while trying to read from enron file: " + enronFiles.get(i));
			}
		}
		
		/* DEBUG
		for (Entry<String, String> entry : kvMap.entrySet()) {
			System.out.println("key: " + entry.getKey() + "\nvalue: " + entry.getValue());
		}
		*/
		
		logger.info("Finished Processing Enron Dataset.\nGenerated KV-Request Pairs: " + kvMap.size());
		populateRequestTables();
	}
	
	private void populateRequestTables() {
		logger.info("Populating Request Tables for all " + clients.size() + " Clients.");
		if (kvMap == null || kvMap.size() <= 0) {
			logger.error("Unable to populate request tables, since request map does not contain any entries.");
			return;
		}
		
		for (ClientWrapper client : clients) {
			// If client not in request table, then insert it
			if (!requestMap.containsKey(client)) {
				requestMap.put(client, new HashMap<String, String>());
			}
			
			// get number of entries from request map
			int numRequestPairs = kvMap.size();
			Random rand = new Random();
			
			
			for (int i = 0; i < numRequestsPerClient; i++) {
				// get a random request pair from request map
				int randomPair = rand.nextInt(numRequestPairs);
				
				// Obtain key for this index
				String key = indexMap.get(randomPair);
				
				// make sure key is valid
				if (key != null && !key.equals("")) {
					// Obtain value for key
					String value = kvMap.get(key);
					
					// add the key value pair to the clients request map
					HashMap<String, String> clientRequests = requestMap.get(client);
					if (clientRequests != null) {
						clientRequests.put(key, value);
					} else {
						logger.error("Unable to add Request with key: \"" + key + "\" and value \"" + value + "\" to\n" +
								"request table for client. The Clients request table was null.");
					}
				} else {
					logger.error("Unable to add random pair with ID: " + randomPair + ". Unable to obtain key\n" +
							"from index map. Size of index map: " + indexMap.size() + ", size of kvMap: " + kvMap.size()); 
				}
			} // end for number of requests
		} // end foreach client
		
		/* DEBUG */
		int i = 0;
		for (ClientWrapper client : clients) {
			i++;
			logger.debug("\nCLIENT " + i + ", REQUEST TABLE:");
			
			for (Entry<String, String> requestEntry : requestMap.get(client).entrySet()) {
				logger.debug("Key: " + requestEntry.getKey() + ", Value: " + requestEntry.getValue());
			}
			logger.debug("_______________________________________");
		}
		
	}
	
	/*
	public void clientsRun(String address, int port) throws ConnectException, UnknownHostException, IOException, InvalidMessageException {
		for (ClientWrapper client : clients) {
			client.connect(address, port);
		}
	}
	*/
	
	public void start(String address, int port) throws ConnectException, UnknownHostException, IOException, InvalidMessageException {	
		clientThreads = new ArrayList<Thread>();
		
		for (ClientWrapper client : clients) {
			client.setRequestMap(requestMap.get(client));
			Thread t = new Thread(client);
			t.setName(client.getName() + " " + t.getName());
			clientThreads.add(t);
			t.start();
		}
	}
	
	public void startServers(int numberOfServers) {
		int currentPort = 50000;
		this.servers = new ArrayList<KVServer>();
		ArrayList<ServerData> serverData = new ArrayList<ServerData>();
		
		
		// for number of servers
		for (int i = 0; i < numberOfServers; i++) {
			
			/* Create Server Data */
			serverData.add(new ServerData("127.0.0.1:" + currentPort, "127.0.0.1", currentPort));
			
			KVServer server = new KVServer(currentPort);
			server.start();
			server.setServeClientRequest(true);
			servers.add(server);
			currentPort++;
		}
		
		logger.info("Server Data: ");
		for (ServerData sd : serverData) {
			logger.info("Name: " + sd.getName() + ", Address: " + sd.getAddress() + ", Port: " + sd.getPort());
		}
		
		for (KVServer server : servers) {
			server.setMetaData(new InfrastructureMetadata(serverData));
		}
	}
	
	public void stopServers() {
		for (KVServer server : servers) {
			server.setRunning(false);
			server.interrupt();
		}
	}
	
	public static void main(String[] args) {
		
		if (args.length != 1) {
			System.out.println("Usage: java perf_eval.Evaluator <PATH_TO_ENRON_DATA>\nThe path must lead to the directory with the names (default: C:\\enron\\maildir\\)");
			System.exit(1);
		}
		
		// Create new Evaluator, first argument is the path to the maildir of enron data
		// second argument is the number of dataPairs read from the dataset.
		Evaluator eval = new Evaluator(args[0], 10, 50000, 1000);
		eval.initEnron();
		eval.startServers(3);
		
		try {
			eval.start("127.0.0.1", 50000);
		} catch (ConnectException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (UnknownHostException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (InvalidMessageException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
		// eval.stopServers();
	}
}
