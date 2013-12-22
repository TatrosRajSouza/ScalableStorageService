package app_kvEcs;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.KVCommunication;
import common.messages.ECSMessage;
import common.messages.ECSStatusType;
import common.messages.InvalidMessageException;
import common.messages.ServerData;

public class ECSServerCommunicator extends ServerData {
	KVCommunication communication;
	private Logger logger;
	/**
	 * Creates a new server data with communication.
	 * @param name The name of the server, used for logging and user I/O
	 * @param address The IP address of the server.
	 * @param port The remote port the server is running on.
	 */
	public ECSServerCommunicator(String name, String address, int port) {
		super(name, address, port);
		communication = null;
		
		LogSetup ls = new LogSetup("logs\\ecs.log", "ECS Comm", Level.ALL);
		this.logger = ls.getLogger();
	}
	
	/**
	 * Initializes communication by establishing a connection to the given address and port.
	 * @throws UnknownHostException Thrown when can't reach the host.
	 * @throws IOException Thrown when there is a problem in the communication.
	 */
	public void connect() throws UnknownHostException, IOException {
		try {
			logger.info("Trying to connect to " + getAddress() + ":" + getPort());
			communication = new KVCommunication(getAddress(), getPort(), "ECS");
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
	public void sendMessage(byte[] msgBytes) throws SocketTimeoutException, IOException {
		ECSStatusType type = null;
		try {
			ECSMessage message = new ECSMessage(msgBytes);
			type = message.getCommand();
		} catch (InvalidMessageException e) {
			logger.debug("Tried to send invalid ECSMessage to " + getAddress() + ":" + getPort());
		}
		
		if (type != null)
			logger.debug("Sending " + type + " to " + getAddress() + ":" + getPort());
		else
			logger.debug("Sending Message of unknown type to " + getAddress() + ":" + getPort());
		
		try {
			communication.sendMessage(msgBytes);
		} catch (Exception e) {
			logger.debug("Unable to send message. Exception: " + e.getMessage());
		}
	}
	
	public ECSMessage receiveMessage() throws SocketTimeoutException, IOException, InvalidMessageException {
		byte[] message = communication.receiveMessage();
		return new ECSMessage(message);
	}
}
