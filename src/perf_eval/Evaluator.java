package perf_eval;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import app_kvClient.KVClient;

public class Evaluator {
	public static final int NUM_CLIENTS = 1;
	public static final String LINE_KEYWORD_KEY = "Date: ";
	public static final String LINE_KEYWORD_VALUE = "Subject: ";
	private File ENRON_DATA;
	
	private ArrayList<KVClient> clients;	// list of clients
	private HashMap<String, String> kvMap;	// key value map with data from enron dataset
	
	public Evaluator(String enronPath) {
		
		
		this.ENRON_DATA= new File(enronPath);
		
		
		for (int i = 0; i < NUM_CLIENTS; i++) {
			clients.add(new KVClient());
		}
	}
	
	public void initEnron() {
		try {
			
			BufferedReader input = new BufferedReader (new FileReader(ENRON_DATA));
			
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
