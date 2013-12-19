package perf_eval;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.HashMap;

import common.messages.InvalidMessageException;

import app_kvClient.KVClient;

public class ClientWrapper implements Runnable {
	public static String defaultServer = "127.0.0.1";
	public static int defaultPort = 50000;
	
	KVClient clientInstance;
	private String name;
	private HashMap<String, String> requestMap;
	
	public ClientWrapper(String name) {
		this.clientInstance = new KVClient(name);
		this.name = name;
	}
	
	public void run() {
		// this.currentThread().setName(this.name + this.currentThread().getName());
		try {
			clientInstance.connect(defaultServer, defaultPort);
			clientInstance.put("A", "B");
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
}
