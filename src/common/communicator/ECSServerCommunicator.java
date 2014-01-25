package common.communicator;

import java.io.IOException;
import java.net.SocketTimeoutException;

import logger.LogSetup;

import org.apache.log4j.Level;

import common.communicator.ServerCommunicator;
import common.messages.ECSMessage;
import common.messages.ECSStatusType;
import common.messages.InvalidMessageException;

public class ECSServerCommunicator extends ServerCommunicator {

	/**
	 * Creates a new server data with communication.
	 * @param name The name of the server, used for logging and user I/O
	 * @param address The IP address of the server.
	 * @param port The remote port the server is running on.
	 */
	public ECSServerCommunicator(String name, String address, int port) {
		super(name, address, port);

		LogSetup ls = new LogSetup("logs/ecs.log", "ECS Comm", Level.ALL);
		logger = ls.getLogger();
	}

	/**
	 * Sends an ECSMessage
	 * @param msgBytes the bytes of the ECSMessage
	 * @throws SocketTimeoutException thrown if the message wasn't delivered on time
	 * @throws IOException Signals that an I/O exception of some sort has occurred
	 */
	public void sendMessage(byte[] msgBytes) throws SocketTimeoutException, IOException {
		ECSStatusType type = null;
		try {
			ECSMessage message = new ECSMessage(msgBytes);
			type = message.getCommand();
		} catch (InvalidMessageException e) {
			logger.debug("Tried to send invalid ECSMessage to " + getAddress() + ":" + getPort());
		}

		sendMessage(msgBytes, type);
	}

	/**
	 * Receive an ECSMessage
	 * @return the ECSMessage received
	 * @throws SocketTimeoutException thrown if couldn't receive the message on time
	 * @throws IOException Signals that an I/O exception of some sort has occurred
	 * @throws InvalidMessageException thrown if the received message wasn't an ECS
	 */
	public ECSMessage receiveMessage() throws SocketTimeoutException, IOException, InvalidMessageException {
		byte[] message = communication.receiveMessage();
		return new ECSMessage(message);
	}
}
