package app_kvServer;

import java.io.IOException;
import java.net.SocketTimeoutException;

import logger.LogSetup;

import org.apache.log4j.Level;

import common.messages.InvalidMessageException;
import common.messages.ServerServerMessage;
import common.messages.ServerServerStatustype;
import app_kvEcs.ServerCommunicator;

public class ServerServerCommunicator extends ServerCommunicator<ServerServerMessage> {

	public ServerServerCommunicator(String address, int port) {
		super(address + ":" + port, address, port);
		
		LogSetup ls = new LogSetup("logs\\server.log", "Server-Server Comm", Level.ALL);
		logger = ls.getLogger();
	}
	
	public void sendMessage(byte[] msgBytes) throws SocketTimeoutException, IOException {
		ServerServerStatustype type = null;
		try {
			ServerServerMessage message = new ServerServerMessage(msgBytes);
			type = message.getCommand();
		} catch (InvalidMessageException e) {
			logger.debug("Tried to send invalid ECSMessage to " + getAddress() + ":" + getPort());
		}
		sendMessage(msgBytes, type);
	}
}
