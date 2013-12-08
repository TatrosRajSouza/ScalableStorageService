package app_kvClient;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.InvalidMessageException;
import common.messages.KVMessage;
import client.KVCommunication;
import client.KVStore;

/**
 * The Client program, which makes use of the KVStore library, 
 * which in turn uses KVCommunication in order to communicate with the KVServer. 
 * @author Elias Tatros
 */
public class KVClient {
	private static Logger logger = Logger.getRootLogger();
	private KVStore kvStore = null;
	KVCommunication connection = null;
	
    /**
     * Main entry point for the KVClient application. 
     */
    public static void main(String[] args) {
    	try {
    		System.setProperty("file.encoding", "US-ASCII");
			new LogSetup("logs/client.log", Level.ALL);
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			// e.printStackTrace();
			System.exit(1);
		} catch (SecurityException ex) {
			System.out.println("Error! Unable to set enconding to ASCII.");
			// ex.printStackTrace();
			System.exit(1);
		}
    	
    	try {
    		Shell shell = new Shell(new KVClient());
    		shell.display();
    	} catch (Exception ex)	{
    		logger.error("A fatal error occured. The program will now exit.");
    		ex.printStackTrace();
    		System.exit(1);
    	}
    }
    
    /**
     * Obtains the logger for the client application
     * @return Logger the logger
     */
    public static Logger getLogger()
    {
    	return logger;
    }
    
    /**
     * Connect to a KVServer
     * @param address the IPv4 address of the server
     * @param port valid port number
     * @throws IOException On communication error
     * @throws UnknownHostException When host address cannot be resolved
     */
	public void connect(String address, int port) throws IOException, UnknownHostException {
		this.kvStore = new KVStore(address, port);
		try {
			kvStore.connect();
		} catch (InvalidMessageException ex) {
			System.out.println("Unable to connect to server. Received an invalid message: \n" + ex.getMessage());
			// ex.printStackTrace();
		}
	}
	
	/**
	 * Disconnects from the current KVServer
	 * @throws ConnectException When not connected or communication error.
	 */
	public void disconnect() throws ConnectException {
		if (kvStore != null)
			kvStore.disconnect();
		else
			throw new ConnectException("Not connected to a KVStore.");
	}
	
	/**
	 * Put a <key,value> pair into the remote KVServer.
	 * @param key the key (any US-ASCII string) of valid length
	 * @param value the value (any US-ASCII string) of valid length
	 * @throws ConnectException If not connected to a KVStore/Server.
	 * @return KVMessage A message that confirms insertion or an error
	 */
	public KVMessage put(String key, String value) throws ConnectException {
		if (kvStore != null)
			return kvStore.put(key, value);
		else
			throw new ConnectException("Not connected to a KVStore.");
	}
	
	/**
	 * Gets the value for a key from the KVServer.
	 * @param key
	 * @return KVMessage the value that is indexed by the given key.
	 * @throws ConnectException
	 */
	public KVMessage get(String key)  throws ConnectException {
		if (kvStore !=null)
			return kvStore.get(key);
		else
			throw new ConnectException("Not connected to a KVStore.");
		
	}
}