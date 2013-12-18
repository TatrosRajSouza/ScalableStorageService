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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;

import app_kvClient.KVClient;

public class Evaluator {
	public static final int NUM_CLIENTS = 1;
	public static final String LINE_KEYWORD_KEY = "Date: ";
	public static final String LINE_KEYWORD_VALUE = "Subject: ";
	private String enronPath = "";
	private String enronFileCachePath = "enronFiles.cache";
	
	private ArrayList<KVClient> clients;	// list of clients
	private HashMap<String, String> kvMap;	// key value map with data from enron dataset
	private HashMap<Integer, String> enronFiles;
	
	public Evaluator(String enronPath) {
		this.enronPath = enronPath;
		
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
	
	public void initEnron() {
		
		try {
			getEnronFiles(enronPath);
		} catch (IllegalArgumentException ex) {
			System.out.println("Unable to initialize enron filepath cache. The program will now exit.");
			System.exit(1);
		}
		
		/*
		for (int i = 0; i < enronFiles.size(); i++) {
			System.out.println(i + ": " + enronFiles.get(i));
		}*/
		
		/*
		BufferedReader input;
		try {
			
			input = new BufferedReader (new FileReader(ENRON_DATA));
			
			try {
				String line = null;
				String valKey = "";
				String valValue = "";
				
				while ((line = input.readLine()) != null) {
					if (line.startsWith(LINE_KEYWORD_KEY) && valKey.equals("")) {
						valKey = line.substring(LINE_KEYWORD_KEY.length());
					} else if (line.startsWith(LINE_KEYWORD_VALUE) && valValue.equals("")) {
						valValue = line.substring(LINE_KEYWORD_VALUE.length());
					}
					
					if (!valKey.equals("") && !valValue.equals(""))
						break;
				}
				
				if (!valKey.equals("") && !valValue.equals("")) {
					if (!kvMap.containsKey(valKey)) {
						kvMap.put(valKey, valValue);
					}
				}
			} finally {
				input.close();
			}
		} catch (IOException ex) {
			System.out.println("IOException occured while trying to initialize enron dataset. (outer)");
		}
		
		*/
	}
	
	public void start() {
		
	}
	
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Usage: java perf_eval.Evaluator <PATH_TO_ENRON_DATA>\nThe path must lead to the directory with the names (default: C:\\enron\\maildir\\)");
			System.exit(1);
		}
		
		Evaluator eval = new Evaluator(args[0]);
		eval.initEnron();
		
	}
}
