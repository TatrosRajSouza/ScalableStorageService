package client;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import common.messages.ServerData;

public class ConsistentHashing {
	MessageDigest md5digest;
	private static Logger logger = Logger.getRootLogger();
	SortedMap<Integer, String> hashCircle;


	public ConsistentHashing() {
		hashCircle =  new TreeMap<Integer, String>();
		
		try {
			md5digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			logger.error("Failed to create MessageDigest. The specified algorithm is not supported.");
			// e.printStackTrace();
		}
	}
	
	public ConsistentHashing(ArrayList<ServerData> servers) {
		hashCircle =  new TreeMap<Integer, String>();
		
		try {
			md5digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			logger.error("Failed to create MessageDigest. The specified algorithm is not supported.");
			// e.printStackTrace();
		}
		
		for (ServerData server : servers) {
			addServer(server.getAddress(), server.getPort());
		}
	}
	
	
	public int hashServer(String address, int port) {
		String hashStr = address + ":" + port;
		try {
			md5digest.reset();
			byte[] bytes = md5digest.digest(hashStr.getBytes("US-ASCII"));
			return byteArrToInt(bytes);
		} catch (UnsupportedEncodingException e) {
			logger.error("Failed to generate hash for Server. The specified encoding is not supported.");
			// e.printStackTrace();
			return 0;
		}
	}
	
	public int hashKey(String key) {
		try {
			md5digest.reset();
			byte[] bytes = md5digest.digest(key.getBytes("US-ASCII"));
			return byteArrToInt(bytes);
		} catch (UnsupportedEncodingException e) {
			logger.error("Failed to generate hash for Server. The specified encoding is not supported.");
			// e.printStackTrace();
			return 0;
		}
	}
	
	public void addServer(String address, int port) {
		String name = address + ":" + port;
		hashCircle.put(hashServer(address, port), name);
	}
	
	public String getServerForKey(String key) {
		if (hashCircle.isEmpty()) {
			return "";
		}
		
		int keyHash = hashKey(key);
		
		if (!hashCircle.containsKey(keyHash)) {
			SortedMap<Integer, String> tailMap = hashCircle.tailMap(keyHash);
			
			if (tailMap.isEmpty())
				return hashCircle.get(hashCircle.firstKey());
			else
				return hashCircle.get(tailMap.firstKey());
		} else {
			return hashCircle.get(keyHash);
		}
	}
	
	public SortedMap<Integer, String> getHashCircle() {
		return hashCircle;
	}
	
	private int byteArrToInt(byte[] b) 
	{
	    return   (b[3] & 0xFF | (b[2] & 0xFF) << 8 | (b[1] & 0xFF) << 16 | (b[0] & 0xFF) << 24);
	}
}
