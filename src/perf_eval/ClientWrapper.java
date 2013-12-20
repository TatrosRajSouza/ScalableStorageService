package perf_eval;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.InvalidMessageException;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

import app_kvClient.KVClient;

public class ClientWrapper implements Runnable {
	public static String defaultServer = "127.0.0.1";
	public static int defaultPort = 50000;
	
	Evaluator evalInstance;
	KVClient clientInstance;
	private String name;
	private HashMap<String, String> requestMap = null;
	private ArrayList<String> keys = null;
	private int putsSent = 0;
	private int putsSuccess = 0;
	private int putsFailed = 0;
	public Logger logger;
	
	public ClientWrapper(String name, Evaluator instance) {
		this.evalInstance = instance;
		this.clientInstance = new KVClient(name);
		this.name = name;
		
		LogSetup ls = new LogSetup("logs\\wrapper.log", "Wrapper " + name, Level.ALL);
		this.logger = ls.getLogger();
	}
	
	public void run() {
		if (requestMap == null) {
			System.out.println(this.getName() + " has an empty request map. Exiting.");
			return;
		}

		try {
			clientInstance.connect(defaultServer, defaultPort);
			
			while (keys.size() > 0) {
				Random rand = new Random();
				int value = rand.nextInt(keys.size());
				
				KVMessage result = clientInstance.put(keys.get(value), requestMap.get(keys.get(value)));
				putsSent++;
				if (result != null && (result.getStatus().equals(StatusType.PUT_SUCCESS) || result.getStatus().equals(StatusType.PUT_UPDATE))) {
					logger.info("Successfully put <" + result.getKey() + ", " + result.getValue() + "> (" + result.getStatus().toString() + ")");
					putsSuccess++;
				} else {
					putsFailed++;
					logger.info("Put Failed <" + result.getKey() + ", " + result.getValue() + "> (" + result.getStatus().toString() + ")");
				}
				
				requestMap.remove(keys.get(value));
				keys.remove(value);
			}
			
			evalInstance.getLogger().info(this.name + " finished. Puts sent: " + putsSent + ", Puts Success: " + putsSuccess + ", Puts failed: " + putsFailed);
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
	}
	
	public KVClient getClient() {
		return this.clientInstance;
	}
	
	public String getName() {
		return this.name;
	}
	
	public void setRequestMap(HashMap<String, String> map) {
		this.requestMap = map;
		
		if (keys == null)
			keys = new ArrayList<String>();
		
		keys.clear();
		for (String key : requestMap.keySet()) {
			keys.add(key);
		}
	}
}
