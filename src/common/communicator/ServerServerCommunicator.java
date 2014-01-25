package common.communicator;

import java.io.IOException;
import java.net.SocketTimeoutException;

import logger.LogSetup;

import org.apache.log4j.Level;

import common.communicator.ServerCommunicator;
import common.messages.InvalidMessageException;
import common.messages.ServerServerMessage;
import common.messages.ServerServerStatustype;

public class ServerServerCommunicator extends ServerCommunicator {

	/**
	 * Creates a new server data with communication.
	 * @param name The name of the server, used for logging and user I/O
	 * @param address The IP address of the server.
	 * @param port The remote port the server is running on.
	 */
	public ServerServerCommunicator(String address, int port) {
		super(address + ":" + port, address, port);
		
		LogSetup ls = new LogSetup("logs\\server.log", "Server-Server Comm", Level.ALL);
		logger = ls.getLogger();
	}
	
	/**
	 * Sends an ServerServerMessage
	 * @param msgBytes the bytes of the ServerServerMessage
	 * @throws SocketTimeoutException thrown if the message wasn't delivered on time
	 * @throws IOException Signals that an I/O exception of some sort has occurred
	 */
	public void sendMessage(byte[] msgBytes) throws SocketTimeoutException, IOException {
		ServerServerStatustype type = null;
		try {
			ServerServerMessage message = new ServerServerMessage(msgBytes);
			type = message.getCommand();
		} catch (InvalidMessageException e) {
			logger.debug("Tried to send invalid ServerServerMessage to " + getAddress() + ":" + getPort());
		}
		sendMessage(msgBytes, type);
	}
}
