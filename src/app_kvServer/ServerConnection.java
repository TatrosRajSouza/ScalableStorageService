package app_kvServer;


import java.math.BigInteger;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.InvalidMessageException;
import common.messages.ServerServerMessage;
import consistent_hashing.ConsistentHashing;

public class ServerConnection {

	public Logger logger;
	private KVServer serverInstance;
	private ServerServerMessage serverServerMessage;

	/**
	 * Creates a processor of server-server messages
	 * @param latestMsg message received that will be processed
	 * @param serverInstance data of the running server 
	 * @throws InvalidMessageException thrown if the message received is invalid
	 */
	public ServerConnection(byte[] latestMsg, KVServer serverInstance) throws InvalidMessageException {
		this.serverServerMessage = new ServerServerMessage(latestMsg);
		this.serverInstance = serverInstance;
		
		LogSetup ls = new LogSetup("logs/server.log", "Server-Server", Level.ALL);
		logger = ls.getLogger();
	}

	/**
	 * Process the message
	 */
	public void process() {
		switch (serverServerMessage.getCommand()) {
		case SERVER_PUT:
			put(serverServerMessage.getNumServer(), serverServerMessage.getKey(), serverServerMessage.getValue());
			break;
		case SERVER_DELETE:
			delete(serverServerMessage.getNumServer(), serverServerMessage.getKey());
			break;
		case SERVER_PUT_ALL:
			putAll(serverServerMessage.getNumServer(), serverServerMessage.getData());
			break;
		}
	}

	private void putAll(int numServer, KVData data) {
		if (numServer == 1) {
			serverInstance.setLastNodeData(data);
		} else {
			serverInstance.setLastLastNodeData(data);
		}
	}

	private void delete(int numServer, String key) {
		if (numServer == 1) {
			serverInstance.getLastNodeData().dataStore.remove(key);
		} else {
			serverInstance.getLastLastNodeData().dataStore.remove(key);
		}
	}

	private void put(int numServer, String key, String value) {
		BigInteger hashedKey = ConsistentHashing.hashKey(serverServerMessage.getKey());
		
		if (numServer == 1) {
			serverInstance.getLastNodeData().put(hashedKey, serverServerMessage.getValue());
		} else {
			serverInstance.getLastLastNodeData().put(hashedKey, serverServerMessage.getValue());
		}
	}
}
