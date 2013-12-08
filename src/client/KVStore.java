package client;


import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import app_kvClient.KVClient;
import app_kvClient.SocketStatus;
import common.messages.InvalidMessageException;
import common.messages.KVMessage;
import common.messages.KVQuery;
import common.messages.KVMessage.StatusType;

/**
 * A library that enables any client application to communicate with a KVServer.
 * @author Elias Tatros
 *
 */
public class KVStore implements KVCommInterface {

	private KVCommunication kvComm;
	private Logger logger;
	private String address;
	private int port;
	private int currentRetries = 0;
	private static final int NUM_RETRIES = 3;
	
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		this.logger = KVClient.getLogger();
	}

	/**
	 * Try to establish Connection to the KVServer.
	 */
	@Override
	public void connect() throws UnknownHostException, IOException, InvalidMessageException {
		kvComm = new KVCommunication(address, port);
		byte[] connectResponse = kvComm.receiveMessage();
		KVQuery kvQueryMessage = new KVQuery(connectResponse);
		
		if (kvQueryMessage.getStatus() == StatusType.CONNECT_SUCCESS) {
			logger.info("Connected to KVServer");
			logger.info("Server Message: " + kvQueryMessage.getTextMessage());
		}
		
		else if (kvQueryMessage.getStatus() == StatusType.CONNECT_ERROR) {
			logger.error("Unable to connect to KVServer.");
		}
		
		else {
			logger.error("Unknown Message received from KVServer. Type: " + kvQueryMessage.getStatus().toString());
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
				logger.error("Unable to send disconnect message, an IO Error occured:\n" + ex.getMessage());
			} catch (InvalidMessageException ex) {
				logger.error("Unable to generate disconnect message, the message type was invalid.");
			}
			
			// TODO: WAIT FOR DISCONNECT_SUCCESS MESSAGE THEN DISCONNECT
			// As this is never sent by the server I simply close the connection for now.
			// Wait for answer
			logger.info("Waiting for disconnect response from server...");
			try {
				byte[] disconnectResponse = kvComm.receiveMessage();
				KVQuery kvQueryMessage = new KVQuery(disconnectResponse);
				if (kvQueryMessage.getStatus() == StatusType.DISCONNECT_SUCCESS) {
					logger.info("Successfully disconnected from server.");
				} else {
					logger.error("No or invalid response from Server, the connection will be closed forcefully.");
				}
			} catch (InvalidMessageException ex) {
				logger.error("Unable to generate KVQueryMessage from Server response:\n" + ex.getMessage());
				// ex.printStackTrace();
			} catch (SocketTimeoutException ex) {
				if (NUM_RETRIES > currentRetries) {
					currentRetries++;
					logger.error("The connection to the KVServer timed out. " +
							"Retrying (" + currentRetries + "/" + NUM_RETRIES + ")");
					this.disconnect();
				} else {
					logger.error("The connection to the KVServer timed out. Closing the connection forcefully.");
				}
			} catch (IOException ex) {
				logger.error("Connection closed forcefully " +
						"because an IO Exception occured while waiting for PUT response from the server:\n" + ex.getMessage());
				// ex.printStackTrace();
			}
			
			if (kvComm.getSocketStatus() == SocketStatus.CONNECTED) {
				kvComm.closeConnection();
			}
			
		} else {
			logger.error("Not connected to a KVServer.");
		}
	}

	/**
	 * Put a key value pair to the remote KVServer
	 * @param key The key that should be inserted
	 * @param value The value associated with the key
	 * @return KVMessage Information retrieved from the server (e.g. if operation successful)
	 */
	@Override
	public KVMessage put(String key, String value) throws ConnectException {
		if (kvComm.getSocketStatus() == SocketStatus.CONNECTED) {
			try {
				kvComm.sendMessage(new KVQuery(StatusType.PUT, key, value).toBytes());
				logger.info("Sent PUT Request for <key, value>: <" + key + ", " + value + ">");
			} catch (IOException ex) {
				logger.error("Unable to send put command, an IO Error occured during transmission:\n" + ex.getMessage());
			} catch (InvalidMessageException ex) {
				logger.error("Unable to generate put command, the message type is invalid for the given arguments.");
			}
			
			// Wait for answer
			logger.info("Waiting for PUT response from server...");
			try {
				byte[] putResponse = kvComm.receiveMessage();
				KVQuery kvQueryMessage = new KVQuery(putResponse);
				KVResult kvResult = new KVResult(kvQueryMessage.getStatus(), kvQueryMessage.getKey(), kvQueryMessage.getValue());
				return kvResult;
			} catch (InvalidMessageException ex) {
				logger.error("Unable to generate KVQueryMessage from Server response:\n" + ex.getMessage());
				// ex.printStackTrace();
			} catch (SocketTimeoutException ex) {
				logger.error("The server did not respond to the PUT Request :(. Please try again at a later time.");
			} catch (IOException ex) {
				logger.error("An IO Exception occured while waiting for PUT response from the server:\n" + ex.getMessage());
				// ex.printStackTrace();
			}
			return null;
		} else {
			throw new ConnectException("Not connected to a KVServer.");
		}
	}

	/**
	 * Obtain the value of a given key from the remote KVServer.
	 * @param key the key that the value should be obtained for
	 * @return KVMessage Information from the server (e.g. GET_SUCESS and value for key if successful, error otherwise).
	 */
	@Override
	public KVMessage get(String key) throws ConnectException {
		if (kvComm.getSocketStatus() == SocketStatus.CONNECTED) {
			try {
				kvComm.sendMessage(new KVQuery(StatusType.GET, key).toBytes());
				logger.info("Sent GET Request for <key>: <" + key + ">");
			} catch (SocketTimeoutException ex) {
				logger.error("Unable to transmit GET Request :(. The connection timed out. Please try again at a later time.");
			} catch (IOException ex) {
				logger.error("Unable to send get command, an IO Error occured during transmission:\n" + ex.getMessage());
			} catch (InvalidMessageException ex) {
				logger.error("Unable to generate get command, the message type is invalid for the given arguments.");
			}
			
			// Wait for answer
			logger.info("Waiting for GET response from server...");
			try {
				byte[] getResponse = kvComm.receiveMessage();
				KVQuery kvQueryMessage = new KVQuery(getResponse);
				KVResult kvResult = new KVResult(kvQueryMessage.getStatus(), kvQueryMessage.getValue(),kvQueryMessage.getKey());
				return kvResult;
			} catch (InvalidMessageException ex) {
				logger.error("Unable to generate KVQueryMessage from Server response:\n" + ex.getMessage());
				// ex.printStackTrace();
			} catch (SocketTimeoutException ex) {
				logger.error("The server did not respond to the GET REquest :(. Please try again at a later time.");
			} catch (IOException ex) {
				logger.error("An IO Exception occured while waiting for GET response from the server:\n" + ex.getMessage());
				// ex.printStackTrace();
			}
			return null;
		} else {
			throw new ConnectException("Not connected to a KVServer.");
		}
	}

	
}
