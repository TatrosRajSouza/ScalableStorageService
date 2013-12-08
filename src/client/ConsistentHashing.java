package client;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

public class ConsistentHashing {
	MessageDigest md5digest;
	private static Logger logger = Logger.getRootLogger();
	
	public ConsistentHashing() {
		try {
			md5digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			logger.error("Failed to create MessageDigest. The specified algorithm is not supported.");
			// e.printStackTrace();
		}
	}
	
	
	public byte[] hashServer(String address, int port) {
		String hashStr = address + ":" + port;
		try {
			md5digest.reset();
			return md5digest.digest(hashStr.getBytes("US-ASCII"));
		} catch (UnsupportedEncodingException e) {
			logger.error("Failed to generate hash for Server. The specified encoding is not supported.");
			// e.printStackTrace();
			return null;
		}
	}
	
	public byte[] hashKey(String key) {
		try {
			md5digest.reset();
			return md5digest.digest(key.getBytes("US-ASCII"));
		} catch (UnsupportedEncodingException e) {
			logger.error("Failed to generate hash for Server. The specified encoding is not supported.");
			// e.printStackTrace();
			return null;
		}
	}
}
