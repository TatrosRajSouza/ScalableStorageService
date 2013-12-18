package perf_eval;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;

import logger.LogSetup;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.log4j.Level;

import common.messages.InfrastructureMetadata;
import common.messages.ServerData;

import app_kvClient.KVClient;
import app_kvEcs.ECS;
import app_kvServer.KVServer;

public class Evaluator {
	public static final int NUM_CLIENTS = 1;
	// Keywords specify which lines we look for in a file from the enron dataset.
	// Most enron files have a Date and Subject tag. So we use the Date as the key and the Subject as the value.
	public static final String LINE_KEYWORD_KEY = "Date: ";
	public static final String LINE_KEYWORD_VALUE = "Subject: ";
	
	private int numKVPairs = 20000;
	private String enronPath = "";
	private String enronFileCachePath = "enronFiles.cache";
	private ArrayList<KVServer> servers;
	
	private ArrayList<KVClient> clients;	// list of clients
	private HashMap<String, String> kvMap;	// key value map with data from enron dataset, keyString -> valueString
	private HashMap<Integer, String> enronFiles; // map: enronfileIDs --> file path
	
	public Evaluator(String enronPath, int numDataPairs) {
		this.enronPath = enronPath;
		this.numKVPairs = numDataPairs;
		
		clients = new ArrayList<KVClient>();
		kvMap = new HashMap<String, String>();
		enronFiles = new HashMap<Integer, String>();
		
		for (int i = 0; i < NUM_CLIENTS; i++) {
			clients.add(new KVClient());
		}
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
			System.out.println("The Enron filepath cache was not found at " + enronFileCachePath);
			
			try {
				fileCache.createNewFile();
			    if (!fileCache.canWrite()) {
			        throw new IllegalArgumentException("Unable to create filepath cache. File cannot be written: " + enronFileCachePath);
			    }
			    
			    System.out.println("Obtaining Enron filepath data from provided path: " + enronPath + ". Please wait...");
			    
				File baseDir = new File(enronPath);
				Collection<File> enronFileList = FileUtils.listFiles(baseDir, null, true);
			
				System.out.println("Building new filepath cache. Please Wait, this can take a while...");
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
				System.out.println("IOException while trying to create filepath cache.\nEnronPath: " + enronPath + "\nFileCachePath: " + enronFileCachePath + "\n" + ex.getMessage());
				ex.printStackTrace();
			}
		}
		
		
		// Get all paths from file cache
		try {
			BufferedReader enronCachedPaths = new BufferedReader(new FileReader(fileCache));
		
			System.out.println("Reading filepaths from file cache. Please Wait, this can take a while...");
			try {
				String line = null;
				
				int i = 0;
				while ((line = enronCachedPaths.readLine()) != null) {
						enronFiles.put(i, line);
						i++;
				}
				
				System.out.println("Enron filepaths initialized. There are " + enronFiles.size() + " files in the set.");
				
			} finally {
				enronCachedPaths.close();
			}
		} catch (IOException ex) {
			System.out.println("IOException while trying to read filepath cache.\nEnronPath: " + enronPath + "\nFileCachePath: " + enronFileCachePath + "\n" + ex.getMessage());
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
			System.out.println("Unable to initialize enron filepath cache. The program will now exit.");
			System.exit(1);
		}
	
		
		BufferedReader contents;
		
		System.out.println("Processing Enron Dataset and generating " + numKVPairs + " data request pairs.");
		System.out.println("Line keyword for keys: " + LINE_KEYWORD_KEY);
		System.out.println("Line keyword for Values: " + LINE_KEYWORD_VALUE);
		System.out.print("Please wait...  ");
		// Iterate over enron file paths
		for (int i = 0; i < enronFiles.size(); i++) {
			try {
				if (kvMap.size() >= numKVPairs)
					break; // If maximum number of KV Pairs in set, stop processing enron data here.
				
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
							kvMap.put(valKey, valValue);
						}
					}
				} finally {
					contents.close();
				}
			} catch (IOException ex) {
				System.out.println("IOException occured while trying to read from enron file: " + enronFiles.get(i));
			}
		}
		
		/* DEBUG
		for (Entry<String, String> entry : kvMap.entrySet()) {
			System.out.println("key: " + entry.getKey() + "\nvalue: " + entry.getValue());
		}
		*/
		
		System.out.println("Finished Processing Enron Dataset.\nGenerated KV-Request Pairs: " + kvMap.size());
	}
	
	public void start() {
		
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
		
		System.out.println("Server Data: ");
		for (ServerData sd : serverData) {
			System.out.println("Name: " + sd.getName() + ", Address: " + sd.getAddress() + ", Port: " + sd.getPort());
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
		
		try {
			new LogSetup("logs/client.log", Level.ALL);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (args.length != 1) {
			System.out.println("Usage: java perf_eval.Evaluator <PATH_TO_ENRON_DATA>\nThe path must lead to the directory with the names (default: C:\\enron\\maildir\\)");
			System.exit(1);
		}
		
		// Create new Evaluator, first argument is the path to the maildir of enron data
		// second argument is the number of dataPairs read from the dataset.
		Evaluator eval = new Evaluator(args[0], 20000);
		eval.initEnron();
		eval.startServers(3);
		eval.stopServers();
	}
}
