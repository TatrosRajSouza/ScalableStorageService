package client;


import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import app_kvClient.KVClient;
import app_kvClient.SocketStatus;
import common.messages.InfrastructureMetadata;
import common.messages.InvalidMessageException;
import common.messages.KVMessage;
import common.messages.KVQuery;
import common.messages.KVMessage.StatusType;
import common.messages.ServerData;
import consistent_hashing.ConsistentHashing;
import consistent_hashing.EmptyServerDataException;

/**
 * A library that enables any client application to communicate with a KVServer.
 * @author Elias Tatros
 *
 */
public class KVStore implements KVCommInterface {

	private KVCommunication kvComm;
	private static Logger logger = Logger.getRootLogger();
	private String address;
	private int port;
	private int currentRetries = 0;
	private static final int NUM_RETRIES = 3;
	private InfrastructureMetadata metaData;
	private ConsistentHashing consHash;
	private String name = "";
	private String moduleName = "<KVStore Module>";
	
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port, String name) {
		this.address = address;
		this.port = port;
		this.name = name;
		
		/* Create empty meta data and add the user-specified server */
		this.metaData = new InfrastructureMetadata();
		this.metaData.addServer("Initial User-Specified Server", address, port);
		// this.metaData.addServer("Second Test Server", address, 50001);
		/* Initialize the consistent Hashing */
		this.consHash = new ConsistentHashing(metaData.getServers());
	}

	/**
	 * Try to establish Connection to the KVServer.
	 */
	@Override
	public void connect() throws UnknownHostException, IOException, InvalidMessageException, ConnectException {
		kvComm = new KVCommunication(address, port, this.name);
		KVQuery kvQueryConnectMessage = new KVQuery(KVMessage.StatusType.CONNECT);
		kvComm.sendMessage(kvQueryConnectMessage.toBytes());
		byte[] connectResponse = kvComm.receiveMessage();
		KVQuery kvQueryMessage = new KVQuery(connectResponse);
		
		if (kvQueryMessage.getStatus() == StatusType.CONNECT_SUCCESS) {
			logger.info(moduleName + ": Connected to KVServer");
			logger.info(moduleName + ": Server Message: " + kvQueryMessage.getTextMessage());
		}
		
		else if (kvQueryMessage.getStatus() == StatusType.CONNECT_ERROR) {
			logger.error(moduleName + ": Unable to connect to KVServer.");
		}
		
		else {
			logger.error(moduleName + ": Unknown Message received from KVServer. Type: " + kvQueryMessage.getStatus().toString());
		}
	}

	/**
	 * Disconnect from the KVServer.
	 */
	@Override
	public void disconnect() {
		if (kvComm != null && kvComm.getSocketStatus() == SocketStatus.CONNECTED) {
			try {
			kvComm.sendMessage(new KVQuery(StatusType.DISCONNECT).toBytes());
			} catch (IOException ex) {
				logger.error(moduleName + ": Unable to send disconnect message, an IO Error occured:\n" + ex.getMessage());
			} catch (InvalidMessageException ex) {
				logger.error(moduleName + ": Unable to generate disconnect message, the message type was invalid.");
			}
			
			// TODO: WAIT FOR DISCONNECT_SUCCESS MESSAGE THEN DISCONNECT
			// As this is never sent by the server I simply close the connection for now.
			// Wait for answer
			logger.info(moduleName + ": Waiting for disconnect response from server...");
			try {
				byte[] disconnectResponse = kvComm.receiveMessage();
				KVQuery kvQueryMessage = new KVQuery(disconnectResponse);
				if (kvQueryMessage.getStatus() == StatusType.DISCONNECT_SUCCESS) {
					logger.info(moduleName + ": Successfully disconnected from server.");
				} else {
					logger.error(moduleName + ": No or invalid response from Server, the connection will be closed forcefully.");
				}
			} catch (InvalidMessageException ex) {
				logger.error(moduleName + ": Unable to generate KVQueryMessage from Server response:\n" + ex.getMessage());
				// ex.printStackTrace();
			} catch (SocketTimeoutException ex) {
				if (NUM_RETRIES > currentRetries) {
					currentRetries++;
					logger.error(moduleName + ": The connection to the KVServer timed out. " +
							"Retrying (" + currentRetries + "/" + NUM_RETRIES + ")");
					this.disconnect();
				} else {
					logger.error(moduleName + ": The connection to the KVServer timed out. Closing the connection forcefully.");
				}
			} catch (IOException ex) {
				logger.error(moduleName + ": Connection closed forcefully " +
						"because an IO Exception occured while waiting for PUT response from the server:\n" + ex.getMessage());
				// ex.printStackTrace();
			}
			
			if (kvComm.getSocketStatus() == SocketStatus.CONNECTED) {
				kvComm.closeConnection();
			}
			
		} else {
			logger.error(moduleName + ": Not connected to a KVServer.");
		}
	}

	/**
	 * Find and connect to the responsible server for a given key according to the current meta data
	 * @param key The key that we want to find the responsible server for
	 * @return ServerData for the responsible Server
	 */
	private ServerData connectResponsibleServer(String key) {
		/* Obtain responsible server according to current meta data */
		ServerData responsibleServer = null;
		try {
			responsibleServer = consHash.getServerForKey(key);

			logger.info(moduleName + ": The responsible Server for key " + key + " is: " + responsibleServer.getAddress() + ":" + responsibleServer.getPort());
			/* Make sure we select the correct server and connect to it */
			if (!(responsibleServer.getAddress().equals(this.address)) || responsibleServer.getPort() != this.port) {
				logger.info(moduleName + ": We are currently not connected to the responsible server (Connected to: " + this.address + ":" + this.port);
				if (kvComm.getSocketStatus() == SocketStatus.CONNECTED) {
					logger.info(moduleName + ": Disconnecting from " + address + ":" + port + " (currently connected Server)");
					this.disconnect();
				}
				
				this.address = responsibleServer.getAddress();
				this.port = responsibleServer.getPort();
				
				try {
					logger.info(moduleName + ": Connecting to Responsible Server: " + address + ":" + port);
					this.connect();
				} catch (UnknownHostException ex) {
					logger.warn(moduleName + ": Put Request Failed. Responsible Server is Unknown Host!");
					// ex.printStackTrace();
				} catch (IOException ex) {
					logger.warn(moduleName + ": Put Request Failed. Could not establish connection due to an IOError!");
					// ex.printStackTrace();
				} catch (InvalidMessageException ex) {
					logger.warn(moduleName + ": Put Request Failed. Unable to connect to responsible server. Received an invalid message: \n" + ex.getMessage());
					// ex.printStackTrace();
				}
			}
		} catch (IllegalArgumentException ex) {
			logger.error(moduleName + ": Failed to obtain responsible server for key " + key + ": The obtained value for the server hash was of invalid format.");
			// ex.printStackTrace();
		} catch (EmptyServerDataException ex) {
			logger.error(moduleName + ": Failed to obtain responsible server for key " + key + ": There are no servers hashed to the circle.");
			// ex.printStackTrace();
		}
		
		return responsibleServer;
	}
	
	/**
	 * Put a key value pair to the remote KVServer
	 * @param key The key that should be inserted
	 * @param value The value associated with the key
	 * @return KVMessage Information retrieved from the server (e.g. if operation successful)
	 */
	@Override
	public KVMessage put(String key, String value) throws ConnectException {
		/* Find & if necessary, connect to responsible Server */
		ServerData responsibleServer = connectResponsibleServer(key);
		
		if (kvComm.getSocketStatus() == SocketStatus.CONNECTED) {
			logger.info(moduleName + ": Connected to the responsible Server: " + address + ":" + port);
			try {
				/* Optimistic Query, send put request to current connected server */
				kvComm.sendMessage(new KVQuery(StatusType.PUT, key, value).toBytes());
				logger.info(moduleName + ": Sent PUT Request for <key, value>: <" + key + ", " + value + ">");
			} catch (IOException ex) {
				logger.error(moduleName + ": Unable to send put command, an IO Error occured during transmission:\n" + ex.getMessage());
			} catch (InvalidMessageException ex) {
				logger.error(moduleName + ": Unable to generate put command, the message type is invalid for the given arguments.");
			}
			
			// Wait for answer
			logger.info("Waiting for PUT response from server...");
			try {
				byte[] putResponse = kvComm.receiveMessage();
				KVQuery kvQueryMessage = new KVQuery(putResponse);
				KVResult kvResult = new KVResult(kvQueryMessage.getStatus(), kvQueryMessage.getKey(), kvQueryMessage.getValue());
				//System.out.println(kvResult.getStatus());
				if (kvResult.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
					/* Need to update meta data and contact other server */
					if (kvResult.key.equals("metaData")) {
						/* Update stale local meta data with actual meta data from server */
						logger.info(moduleName + ": Received new MetaData from Server: " + kvResult.value);
						this.metaData.update(kvResult.value);
						/* Update consistent hashing circle to new version */
						this.consHash.update(metaData.getServers());
						/* Retrieve & connect responsible Server for put key */
						responsibleServer = connectResponsibleServer(key);
						if (responsibleServer == null) {
							logger.error(moduleName + ": Put Request Failed. Unable to find responsible server for key: " + key + "\nList of servers in circle: \n");
							for (String server : consHash.getHashCircle().values()) {
								logger.error(server);
							}
							return null;
						}
						/* Retry PUT */
						return this.put(key, value);
					}
				}
				
				// System.out.println(kvResult.getStatus());
				// PUT_SUCCESS or PUT_UPDATE
				return kvResult;
			} catch (InvalidMessageException ex) {
				logger.error(moduleName + ": Unable to generate KVQueryMessage from Server response:\n" + ex.getMessage());
				// ex.printStackTrace();
			} catch (SocketTimeoutException ex) {
				logger.error(moduleName + ": The server did not respond to the PUT Request :(. Please try again at a later time.");
			} catch (IOException ex) {
				logger.error(moduleName + ": An IO Exception occured while waiting for PUT response from the server:\n" + ex.getMessage());
				// ex.printStackTrace();
			} catch (IllegalArgumentException ex) {
				logger.error(moduleName + ": Failed to obtain responsible server for key " + key + ": The obtained value for the server hash was of invalid format.");
				// ex.printStackTrace();
			}
			return null;
		} else {
			throw new ConnectException(moduleName + ": Not connected to a KVServer.");
		}
	}

	/**
	 * Obtain the value of a given key from the remote KVServer.
	 * @param key the key that the value should be obtained for
	 * @return KVMessage Information from the server (e.g. GET_SUCESS and value for key if successful, error otherwise).
	 */
	@Override
	public KVMessage get(String key) throws ConnectException {
		/* Find & if necessary, connect to responsible Server */
		ServerData responsibleServer = connectResponsibleServer(key);
		
		if (kvComm.getSocketStatus() == SocketStatus.CONNECTED) {
			/* Optimistic query to currently connected Server */
			try {
				kvComm.sendMessage(new KVQuery(StatusType.GET, key).toBytes());
				logger.info(moduleName + ": Sent GET Request for <key>: <" + key + ">");
			} catch (SocketTimeoutException ex) {
				logger.error(moduleName + ": Unable to transmit GET Request :(. The connection timed out. Please try again at a later time.");
			} catch (IOException ex) {
				logger.error(moduleName + ": Unable to send get command, an IO Error occured during transmission:\n" + ex.getMessage());
			} catch (InvalidMessageException ex) {
				logger.error(moduleName + ": Unable to generate get command, the message type is invalid for the given arguments.");
			}
			
			// Wait for answer
			logger.info("Waiting for GET response from server...");
			try {
				byte[] getResponse = kvComm.receiveMessage();
				KVQuery kvQueryMessage = new KVQuery(getResponse);
				KVResult kvResult = new KVResult(kvQueryMessage.getStatus(), kvQueryMessage.getValue(),kvQueryMessage.getKey());
				
				if (kvResult.getStatus() == StatusType.GET_SUCCESS || kvResult.getStatus() == StatusType.GET_ERROR) {
					return kvResult;
				} else if (kvResult.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
					/* Need to update meta data and contact other server */
					if (kvResult.key.equals("metaData")) {
						/* Update stale local meta data with actual meta data from server */
						logger.info(moduleName + ": Received new MetaData from Server: " + kvResult.value);
						this.metaData.update(kvResult.value);
						/* Update consistent hashing circle to new version */
						this.consHash.update(metaData.getServers());
						/* Retrieve & connect responsible Server for put key */
						this.connectResponsibleServer(key);
						if (responsibleServer == null) {
							logger.error(moduleName + ": Get Request Failed. Unable to find responsible server for key: " + key + "\nList of servers in circle: \n");
							for (String server : consHash.getHashCircle().values()) {
								logger.error(server);
							}
							return null;
						}
						/* Retry GET */
						this.get(key);
					} else {
						throw new InvalidMessageException(moduleName + ": Invalid Response Message received from Server:\n" +
								"  Type: " + kvResult.getStatus() + "\n" +
								"  Key: " + kvResult.getKey() + "\n" +
								"  Value: " + kvResult.getValue());
					}
				}
			} catch (InvalidMessageException ex) {
				logger.error(moduleName + ": Unable to generate KVQueryMessage from Server response:\n" + ex.getMessage());
				// ex.printStackTrace();
			} catch (SocketTimeoutException ex) {
				logger.error(moduleName + ": The server did not respond to the GET REquest :(. Please try again at a later time.");
			} catch (IOException ex) {
				logger.error(moduleName + ": An IO Exception occured while waiting for GET response from the server:\n" + ex.getMessage());
				// ex.printStackTrace();
			} catch (IllegalArgumentException ex) {
				logger.error(moduleName + ": Failed to obtain responsible server for key " + key + ": The obtained value for the server hash was of invalid format.");
				// ex.printStackTrace();
			}
			return null;
		} else {
			throw new ConnectException("Not connected to a KVServer.");
		}
	}
	
	/**
	 * Obtain the current meta data for this KVStore
	 * @return {@link InfrastructureMetadata} The meta data for this instance of KVStore
	 */
	public InfrastructureMetadata getMetadata() {
		return this.metaData;
	}
	
	/**
	 * Obtain the current Hash-circle for this instance of KVStore
	 * @return {@link ConsistentHashing} current Hash-circle for this instance of KVStore
	 */
	public ConsistentHashing getHashCircle() {
		return this.consHash;
	}
	
	/**
	 * Obtain connection status of communication module
	 * @return {@link SocketStatus} connection status of communication module
	 */
	public SocketStatus getConnectionStatus() {
		if (this.kvComm != null)
			return this.kvComm.getSocketStatus();
		else {
			logger.error(moduleName + ": Cannot obtain socket status. Communication module not initialized.");
			return SocketStatus.DISCONNECTED;
		}
			
	}
}
