package app_kvClient;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.InfrastructureMetadata;
import common.messages.InvalidMessageException;
import common.messages.KVMessage;
import consistent_hashing.ConsistentHashing;
import client.KVCommunication;
import client.KVStore;

/**
 * The Client program, which makes use of the KVStore library, 
 * which in turn uses KVCommunication in order to communicate with the KVServer. 
 * @author Elias Tatros
 */
public class KVClient {
	public static final boolean DEBUG = true;
	private Logger logger;
	// private static Logger rootLogger = Logger.getRootLogger();
	private KVStore kvStore = null;
	KVCommunication connection = null;
	String name = "Client";
		
	public KVClient() {
		initLog();
	}
	
	public KVClient(String name) {
		this.name = name;
		initLog();
	}
	
	public void initLog() {
		LogSetup ls = new LogSetup("logs\\client.log", name, Level.ALL);
		this.logger = ls.getLogger();
	}
	
	/**
     * Main entry point for the KVClient application. 
     */
    public static void main(String[] args) {
    	System.setProperty("file.encoding", "US-ASCII");
    	
    	try {
    		Shell shell = new Shell(new KVClient("ShellClient"));
    		shell.display();
    	} catch (Exception ex)	{
    		System.out.println("A fatal error occured. The program will now exit.");
    		ex.printStackTrace();
    		System.exit(1);
    	}
    }
    
    /**
     * Obtains the logger for the client application
     * @return Logger the logger
     */
    public Logger getLogger()
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
	public void connect(String address, int port) throws IOException, ConnectException, UnknownHostException, InvalidMessageException {
		this.kvStore = new KVStore(address, port, this.name);
		kvStore.connect();
	}
	
	/**
	 * Disconnects from the current KVServer
	 * @throws ConnectException When not connected or communication error.
	 */
	public void disconnect() throws ConnectException {
		if (kvStore != null)
			kvStore.disconnect();
		else
			throw new ConnectException(" Not connected to a KVStore.");
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
			throw new ConnectException(" Not connected to a KVStore.");
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
			throw new ConnectException(" Not connected to a KVStore.");
		
	}
	
	/**
	 * Obtain the current meta data for this Client
	 * @return {@link InfrastructureMetadata} The meta data for this client instance
	 */
	public InfrastructureMetadata getMetadata() {
		if (kvStore != null) {
			return this.kvStore.getMetadata();
		} else {
			logger.error(" Cannot obtain meta data from client.");
			return null;
		}
	}
	
	/**
	 * Obtain the current consistent hash-circle for this Client
	 * @return {@link InfrastructureMetadata} The meta data for this client instance
	 */
	public ConsistentHashing getHashCircle() {
		if (kvStore != null) {
			return this.kvStore.getHashCircle();
		} else {
			logger.error(" Cannot obtain hash-circle data from client.");
			return null;
		}
	}
	
	/**
	 * Obtain Status of the client connection
	 * @return {@link SocketStatus} the current socket state
	 */
	public SocketStatus getConnectionStatus() {
		return this.kvStore.getConnectionStatus();
	}
	
	/**
	 * Optional: get the name of this client, empty if not set before
	 * @return clients name
	 */
    public String getClientName() {
		return name;
	}

    /**
     * Optional: set the name of this client
     * @param name clients name
     */
	public void setClientName(String name) {
		this.name = name;
	}
}