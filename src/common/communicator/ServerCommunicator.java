package common.communicator;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import common.ServerData;
import client.KVCommunication;

public abstract class ServerCommunicator<T> extends ServerData {

	protected Logger logger;
	protected KVCommunication communication;
	
	public ServerCommunicator(String name, String address, int port) {
		super(name, address, port);
		communication = null;
	}

	/**
	 * Initializes communication by establishing a connection to the given address and port.
	 * @throws UnknownHostException Thrown when can't reach the host.
	 * @throws IOException Thrown when there is a problem in the communication.
	 */
	public void connect() throws UnknownHostException, IOException {
		try {
			logger.info("Trying to connect to " + getAddress() + ":" + getPort());
			communication = new KVCommunication(getAddress(), getPort(), "Server-Server");
			logger.info("Connected to " + getAddress() + ":" + getPort());
		} catch (UnknownHostException e) {
			logger.error("Error! Couldn't connect to " + getAddress() + ":" + getPort());
		} catch (IOException e) {
			logger.error("Error! IOException while trying to connect to " + getAddress() + ":" + getPort());
			try {
				Thread.sleep(100);
			} catch (InterruptedException e1) {
				logger.warn("Warn! Thread interrupted by other thread.");
			}
			connect();
		}
	}
	
	/**
	 * Gracefully closes the connection to the KVServer.
	 */
	public void disconnect() {
		communication.closeConnection();
	}

	/**
	 * Sends a byte[] message using this socket.
	 * @param msgBytes The message that is to be sent.
	 * @throws SocketTimeoutException Couldn't deliver the message in the expected time.
	 * @throws IOException Thrown when there is a problem in the communication.
	 */
	protected void sendMessage(byte[] msgBytes, Object type) throws SocketTimeoutException, IOException {
		if (type != null)
			logger.debug("Sending " + type + " to " + getAddress() + ":" + getPort());
		else
			logger.debug("Sending Message of unknown type to " + getAddress() + ":" + getPort());

		try {
			communication.sendMessage(msgBytes);
		} catch (Exception e) {
			logger.debug("Unable to send message. Exception: " + e.getClass().getName());
		}
	}
}
