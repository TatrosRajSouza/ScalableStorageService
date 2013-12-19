package app_kvEcs;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import client.KVCommunication;
import common.messages.ECSMessage;
import common.messages.InvalidMessageException;
import common.messages.ServerData;

public class ECSServerCommunicator extends ServerData {
	KVCommunication communication;
	
	/**
	 * Creates a new server data with communication.
	 * @param name The name of the server, used for logging and user I/O
	 * @param address The IP address of the server.
	 * @param port The remote port the server is running on.
	 */
	public ECSServerCommunicator(String name, String address, int port) {
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
			communication = new KVCommunication(getAddress(), getPort(), "ECS");
		} catch (UnknownHostException e) {
			ECS.logger.error("Error! Couldn't connect to " + getAddress() + ":" + getPort());
		} catch (IOException e) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e1) {
				ECS.logger.warn("Warn! Thread interrupted by other thread.");
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
		communication.sendMessage(msgBytes);
	}
	
	public ECSMessage receiveMessage() throws SocketTimeoutException, IOException, InvalidMessageException {
		byte[] message = communication.receiveMessage();
		return new ECSMessage(message);
	}
}
