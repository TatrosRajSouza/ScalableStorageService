package perf_eval;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

import common.messages.InvalidMessageException;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

import app_kvClient.KVClient;

public class ClientWrapper implements Runnable {
	public static String defaultServer = "127.0.0.1";
	public static int defaultPort = 50000;
	
	KVClient clientInstance;
	private String name;
	private HashMap<String, String> requestMap = null;
	private ArrayList<String> keys = null;
	
	public ClientWrapper(String name) {
		this.clientInstance = new KVClient(name);
		this.name = name;
	}
	
	public void run() {
		if (requestMap == null) {
			System.out.println(this.getName() + " has an empty request map. Exiting.");
			return;
		}

		try {
			clientInstance.connect(defaultServer, defaultPort);
			KVMessage result = clientInstance.put(keys.get(0), requestMap.get(keys.get(0)));
			if (result != null && (result.getStatus().equals(StatusType.PUT_SUCCESS) || result.getStatus().equals(StatusType.PUT_SUCCESS))) {
				System.out.println(this.name + ": Successfully put <" + result.getKey() + ", " + result.getValue() + "> (" + result.getStatus().toString() + ")");
			}
			
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
