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
	
	private void getEnronFiles(String enronPath) {
		File fileCache = new File("enronFiles.cache");
		
	    if (fileCache == null) {
	        throw new IllegalArgumentException("File should not be null.");
	    }
	    
	    if (fileCache.exists() && !fileCache.isFile()) {
	        throw new IllegalArgumentException("Should not be a directory: " + fileCache);
	    }
	    

	    
		if (!fileCache.exists()) { // build the enron file cache if it doesn't exist
			try {
				fileCache.createNewFile();
			    if (!fileCache.canWrite()) {
			        throw new IllegalArgumentException("File cannot be written: " + fileCache);
			    }
			    
				File baseDir = new File(enronPath);
				Collection<File> enronFileList = FileUtils.listFiles(baseDir, null, true);
			
				Writer output = new BufferedWriter(new FileWriter(fileCache));
			
				try {
					for (File file : enronFileList) {
				      //Write paths to file
				      output.write(file.getAbsolutePath() + "\n");
					}
				} finally {
				      output.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		// Get all paths from file cache
		try {
			BufferedReader enronCachedPaths = new BufferedReader(new FileReader(fileCache));
		
			try {
				String line = null;
				
				int i = 0;
				while ((line = enronCachedPaths.readLine()) != null) {
						enronFiles.put(i, line);
						i++;
				}
				
			} finally {
				enronCachedPaths.close();
			}
		} catch (IOException ex) {
			
		}
	}
	
	public void initEnron() {
		getEnronFiles(enronPath);
		
		for (int i = 0; i < enronFiles.size(); i++) {
			System.out.println(i + ": " + enronFiles.get(i));
		}
		
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
