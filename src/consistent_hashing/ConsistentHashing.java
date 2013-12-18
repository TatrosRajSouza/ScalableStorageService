package consistent_hashing;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import common.messages.ServerData;

/**
 * Implements Consistent Hashing of Servers and Keys
 * Hashes Servers and Keys (Strings) to 128 bit md5 hashes.
 * Hashes for Servers are stored on a circle (SortedMap hashCircle).
 * The responsible server for a key is the next larger hash in clock-wise direction
 * and can be obtained by calling getServerForKey(String key) 
 * @author Elias Tatros
 *
 */
public class ConsistentHashing {
	MessageDigest md5digest;
	private static Logger logger;
	SortedMap<BigInteger, String> hashCircle;

	/**
	 * Enables Consistent Hashing, start with empty circle
	 */
	public ConsistentHashing() {
		hashCircle =  Collections.synchronizedSortedMap(new TreeMap<BigInteger, String>());
		logger = Logger.getRootLogger();

		try {
			md5digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			logger.error("Failed to create MessageDigest. The specified algorithm is not supported.");
			// e.printStackTrace();
		}
	}

	/**
	 * Enables Consistent Hashing, start with a number of servers hashed to the circle
	 * @param servers An ArrayList of ServerData that should be hashed to the circle
	 */
	public ConsistentHashing(ArrayList<ServerData> servers) {
		hashCircle =  Collections.synchronizedSortedMap(new TreeMap<BigInteger, String>());
		logger = Logger.getRootLogger();

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

	/**
	 * For internal use, generates 128-bit md5 hash for <address>:<port> of a server 
	 * @param address IP Address of the server
	 * @param port Remote port number
	 * @return 128-bit md5 hash
	 */
	private BigInteger hashServer(String address, int port) {
		String hashStr = address + ":" + port;
		try {
			md5digest.reset();
			byte[] bytes = md5digest.digest(hashStr.getBytes("US-ASCII"));
			return new BigInteger(1,bytes);
			//return byteArrToInt(bytes);
		} catch (UnsupportedEncodingException e) {
			logger.error("Failed to generate hash for Server. The specified encoding is not supported.");
			// e.printStackTrace();
			return BigInteger.ZERO;
		}
	}

	/**
	 * For internal use, generates 128-bit md5 hash for a String key 
	 * @param key A String key that should be hashed
	 * @return 128-bit md5 hash
	 */
	public BigInteger hashKey(String key) {
		try {
			md5digest.reset();
			byte[] bytes = md5digest.digest(key.getBytes("US-ASCII"));
			return new BigInteger(1,bytes);
		} catch (UnsupportedEncodingException e) {
			logger.error("Failed to generate hash for Server. The specified encoding is not supported.");
			// e.printStackTrace();
			return BigInteger.ZERO;
		}
	}

	/**
	 * Add a new server, hashing it to the circle
	 * @param address IP address of the server
	 * @param port Remote port number of the server
	 */
	public void addServer(String address, int port) {
		String name = address + ":" + port;
		hashCircle.put(hashServer(address, port), name);
	}
	
	/**
	 * Remove a new server, hashing it to the circle
	 * @param address IP address of the server
	 * @param port Remote port number of the server
	 */
	public void removeServer(String address, int port) {
		String name = address + ":" + port;

		for (BigInteger hash : hashCircle.keySet()) {
			if (hashCircle.get(hash).equals(name)) {
				hashCircle.remove(hash);
			}
		}
	}

	/**
	 * Clear all server hashes from circle and replace with new data provided
	 * @param servers List of servers to hash and add to the circle 
	 */
	public void update(ArrayList<ServerData> servers) {
		hashCircle.clear();

		if (servers != null && servers.size() > 0)
		{
			for (ServerData server : servers) {
				addServer(server.getAddress(), server.getPort());
			}
		} else {
			logger.warn("The meta-data that was supplied to the consistent hash update has no servers, resulting in an empty HashCircle!");
		}
	}

	/**
	 * Obtain the responsible server for a certain key
	 * @param key The String key you want to obtain the server for
	 * @return The responsible server for the provided key
	 * @throws EmptyServerDataException 
	 */
	public ServerData getServerForKey(String key) throws EmptyServerDataException, IllegalArgumentException {
		// logger.debug("Trying to obtain Server for key " + key);

		/* Make sure we have Servers on the circle */
		if (hashCircle.isEmpty()) {
			throw new EmptyServerDataException("There are no Servers in the hashCircle");
		}

		BigInteger keyHash = hashKey(key); // obtain hash of the provided key
		// logger.debug("Key " + key + " hashed to " + keyHash);

		if (!hashCircle.containsKey(keyHash)) { // hash of key not in circle? -> Find next larger server hash in clock-wise direction
			SortedMap<BigInteger, String> tailMap = hashCircle.tailMap(keyHash); // Obtain the tailMap for the key hash

			if (tailMap.isEmpty()) { // TailMap was empty, hence Wrap-around and return the first server in the map
				try {
					return ServerDataFromValue(hashCircle.get(hashCircle.firstKey()));
				} catch (IllegalArgumentException ex) {
					logger.error("Hash value for key " + hashCircle.firstKey() + " of invalid format: " + hashCircle.get(hashCircle.firstKey()));
					throw ex;
				}
			}
			else { // Return the first server in the TailMap
				try {
					return ServerDataFromValue(hashCircle.get(tailMap.firstKey()));
				} catch (IllegalArgumentException ex) {
					logger.error("Hash value for key " + tailMap.firstKey() + " of invalid format: " + hashCircle.get(tailMap.firstKey()));
					throw ex;
				}
			}
		} else { // There is a server with equal hash to the key
			try {
				return ServerDataFromValue(hashCircle.get(keyHash));
			} catch (IllegalArgumentException ex) {
				logger.error("Hash value for key " + keyHash + " of invalid format: " + hashCircle.get(keyHash));
				throw ex;
			}
		}
	}

	/**
	 * Internal use, create instance of ServerData for a value in the hash-circle
	 * @param value A value for a server on the hash-circle ("<IP>:<Port>")
	 * @return The ServerData corresponding to the provided value
	 */
	private ServerData ServerDataFromValue(String value) throws IllegalArgumentException {
		String serverStr[] = value.split(":");

		if (serverStr.length == 2) {
			String serverName = serverStr[0] + ":" + serverStr[1];
			String serverAddress = serverStr[0];
			int serverPort = Integer.parseInt(serverStr[1]);
			return new ServerData(serverName, serverAddress, serverPort);
		} else {
			throw new IllegalArgumentException(value + " is not of the valid format <IP>:<port>");
		}
	}

	/**
	 * Obtain the hash-circle
	 * @return The hash-circle as a SortedMap
	 */
	public SortedMap<BigInteger, String> getHashCircle() {
		return hashCircle;
	}
}
