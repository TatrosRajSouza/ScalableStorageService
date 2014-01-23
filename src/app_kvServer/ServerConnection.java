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

	public ServerConnection(byte[] latestMsg, KVServer serverInstance) throws InvalidMessageException {
		this.serverServerMessage = new ServerServerMessage(latestMsg);
		this.serverInstance = serverInstance;
		
		LogSetup ls = new LogSetup("logs\\server.log", "Server-Server", Level.ALL);
		logger = ls.getLogger();
	}

	public ServerServerMessage process() {
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
		
		/*if(ecsMessage.getCommand().equals(ECSStatusType.MOVE_DATA_INTERNAL))
		{
			move = "error";
			move = moveData(ecsMessage.getMovingData());
		}
		else if(ecsMessage.getCommand().equals(ECSStatusType.MOVE_DATA))
		{
			try {
				move = "error";
				move = moveData(ecsMessage.getStartIndex(), ecsMessage.getEndIndex(), ecsMessage.getServer());
			} catch (UnknownHostException e) {
				logger.error("Error while updation"+e.getMessage());
			} catch (IOException e) {
				logger.error("Error while updation"+e.getMessage());
			} catch (InvalidMessageException e) {
				logger.error("Error while updation"+e.getMessage());
			}
		}

		return move;*/
		return serverServerMessage;
	}

	private void putAll(int numServer, KVData data) {
		if (numServer == 1) {
			serverInstance.lastNodeData = data;
		} else {
			serverInstance.lastLastNodeData = data;
		}
	}

	private void delete(int numServer, String key) {
		if (numServer == 1) {
			serverInstance.lastNodeData.dataStore.remove(key);
		} else {
			serverInstance.lastLastNodeData.dataStore.remove(key);
		}
	}

	private void put(int numServer, String key, String value) {
		BigInteger hashedKey = ConsistentHashing.hashKey(serverServerMessage.getKey());
		
		if (numServer == 1) {
			serverInstance.lastNodeData.put(hashedKey, serverServerMessage.getValue());
		} else {
			serverInstance.lastLastNodeData.put(hashedKey, serverServerMessage.getValue());
		}
	}
}
