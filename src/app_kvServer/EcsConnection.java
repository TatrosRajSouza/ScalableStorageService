package app_kvServer;

import java.io.IOException;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.SortedMap;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.KVCommunication;
import common.messages.ECSMessage;
import common.messages.ECSStatusType;
import common.messages.InfrastructureMetadata;
import common.messages.InvalidMessageException;
import common.messages.ServerData;
import common.messages.ServerServerMessage;
import common.messages.ServerServerStatustype;
import consistent_hashing.ConsistentHashing;
/**
 * Represents a connection end point for a particular ECS that is 
 * connected to the server. It provides server admin interface like start,stop,setwritelock etc
 * @author Udhayaraj Sivalingam 
 */
public class EcsConnection {
	private static Logger logger;
	private String move;
	private ECSMessage ecsMessage;
	private KVServer serverInstance;
	public EcsConnection(byte[] latestMsg,KVServer serverInstance) throws InvalidMessageException {
		this.ecsMessage = new ECSMessage(latestMsg);
		this.serverInstance = serverInstance;

		LogSetup ls = new LogSetup("logs\\server.log", "Server", Level.ALL);
		EcsConnection.logger = ls.getLogger();
	}
	public String process() throws InvalidMessageException {
		logger.debug("Received message from ECS: " + ecsMessage.getCommand());
		if(ecsMessage.getCommand().equals(ECSStatusType.INIT)) {	
			initKVServer(ecsMessage.getMetadata());
		}
		else if(ecsMessage.getCommand().equals(ECSStatusType.START))
			start();
		else if(ecsMessage.getCommand().equals(ECSStatusType.STOP))
			stopServer();
		else if(ecsMessage.getCommand().equals(ECSStatusType.SHUTDOWN))
			shutDown();
		else if(ecsMessage.getCommand().equals(ECSStatusType.LOCK_WRITE))
			lockWrite();
		else if(ecsMessage.getCommand().equals(ECSStatusType.UNLOCK_WRITE))
			UnLockWrite();
		else if(ecsMessage.getCommand().equals(ECSStatusType.UPDATE))
			update(ecsMessage.getMetadata());
		else if(ecsMessage.getCommand().equals(ECSStatusType.MOVE_DATA_INTERNAL))
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
		else if (ecsMessage.getCommand().equals(ECSStatusType.GET_STATUS)) {
			getStatus();
		}

		return move;
	}

	private void getStatus() {
		logger.info("Server data (coordinator): " + serverInstance.getKvdata());
		logger.info("Server data (replica - last node): " + serverInstance.lastNodeData);
		logger.info("Server data (replica - last last node):" + serverInstance.lastLastNodeData);
	}
	
	public ECSStatusType getCommand() {
		return ecsMessage.getCommand();
	}

	private String moveData(HashMap<BigInteger, String> movingData) {
		this.serverInstance.getKvdata().moveData(movingData);
		return "moveinternalcompleted";
	}
	/**
	 * Initializes and starts the server but blocked for client requests
	 */
	// convert with meta data parameter
	private void initKVServer(InfrastructureMetadata metaData)
	{
		logger.info("Received INIT from ECS. Setting meta data to " + metaData.toString());
		this.serverInstance.setMetaData(metaData);
		logger.info("Set meta data to " + this.serverInstance.getMetaData().toString());

		changeNextServers();
	}

	private void changeNextServers() {
		String oldNextServer = "";
		String oldNextNextServer = "";
		String nextServer = "";
		String nextNextServer = "";
		SortedMap<BigInteger, String> hashCircle;
		int next = 0;

		if (serverInstance.nextServer != null) {
			oldNextServer = serverInstance.nextServer.getName();
		} if (serverInstance.nextNextServer != null) {
			oldNextServer = serverInstance.nextNextServer.getName();
		}
		hashCircle = serverInstance.getConsistentHashing().getHashCircle();
		for (BigInteger hash : hashCircle.keySet()) {
			if (next > 0) {
				if (next == 2) {
					nextNextServer = hashCircle.get(hash);
					next = 0;
					break;
				}
				nextServer = hashCircle.get(hash);
				next++;
			} else if (hashCircle.get(hash).equals(serverInstance.getServerData().getName())) {
				next++;
			}
		}

		Object[] hashArray = hashCircle.keySet().toArray();
		switch (next) {
		case 1:
			nextServer = hashCircle.get(hashArray[0]);
			try {
				nextNextServer = hashCircle.get(hashArray[1]);
			} catch (ArrayIndexOutOfBoundsException ex) {
				nextNextServer = hashCircle.get(hashArray[0]);
			}
			break;
		case 2:
			nextNextServer = hashCircle.get(hashArray[0]);
			break;
		}

		setNextServer(nextServer, nextNextServer);
		sendMessageToNextServers(oldNextServer, oldNextNextServer, nextServer, nextNextServer);
	}
	private void sendMessageToNextServers(String oldNextServer, String oldNextNextServer, String nextServer, String nextNextServer) {
		if (serverInstance.nextServer != null && !nextServer.equals(oldNextServer) && !nextServer.equals(oldNextNextServer)) {
			logger.info("%%% " + serverInstance.getKvdata().toString().length());
			ServerServerMessage message = new ServerServerMessage(ServerServerStatustype.SERVER_PUT_ALL, 1, serverInstance.getKvdata());
			try {
				serverInstance.nextServer.sendMessage(message.toBytes());
			} catch (SocketTimeoutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (serverInstance.nextNextServer != null && !nextNextServer.equals(oldNextServer) && !nextNextServer.equals(oldNextNextServer)) {
			logger.info("%%%");
			ServerServerMessage message = new ServerServerMessage(ServerServerStatustype.SERVER_PUT_ALL, 2, serverInstance.getKvdata());
			try {
				serverInstance.nextNextServer.sendMessage(message.toBytes());
			} catch (SocketTimeoutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void setNextServer(String nextServer, String nextNextServer) {
		if (nextServer.equals(serverInstance.getServerData().getName())) {
			serverInstance.nextServer = null;
		} else {
			String[] name = nextServer.split(":");
			serverInstance.nextServer = new ServerServerCommunicator(name[0], Integer.parseInt(name[1]));
			try {
				serverInstance.nextServer.connect();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (nextNextServer.equals(serverInstance.getServerData().getName())) {
			serverInstance.nextNextServer = null;
		} else {
			String[] name = nextServer.split(":");
			serverInstance.nextNextServer = new ServerServerCommunicator(name[0], Integer.parseInt(name[1]));
			try {
				serverInstance.nextNextServer.connect();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private  void start() {
		logger.info("Allowing server to serve client requests");
		this.serverInstance.setServeClientRequest(true);
	}
	private void stopServer(){		
		logger.info("Stopping server to serve client requests");
		this.serverInstance.setServeClientRequest(false);
	}
	private void lockWrite()
	{
		logger.info("locking server to write requests");
		this.serverInstance.setWriteLocked(true);
	}
	private void UnLockWrite()
	{
		logger.info("unlocking server to write requests");
		this.serverInstance.setWriteLocked(false);
	}

	private void shutDown(){
		this.serverInstance.setRunning(false);
		try {
			this.serverInstance.getServerSocket().close();
			System.exit(0);
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + this.serverInstance.getPort(), e);
			System.exit(1);
		}

	}
	private void update(InfrastructureMetadata infrastructureMetadata)
	{
		this.serverInstance.getMetaData().update(infrastructureMetadata.toString());
		this.serverInstance.getConsistentHashing().update(infrastructureMetadata.getServers());
		//logger.info("metadata updation:" + infrastructureMetadata.toString());
		changeNextServers();
	}
	private String moveData(BigInteger startIndex, BigInteger endIndex, ServerData serverData) throws UnknownHostException, IOException, InvalidMessageException
	{
		//logger.info("Data moved to:" + serverData.getName() + "port:" + serverData.getPort());
		HashMap<BigInteger, String> movingData;
		BigInteger serverIndex = ConsistentHashing.hashServer(serverData.getAddress(), serverData.getPort());
		//logger.info("Start Index:" + startIndex);
		//logger.info("End Index:" + endIndex);
		//logger.info("serverIndex:" + serverIndex);
		if(startIndex.compareTo(endIndex) >= 0)
		{
			if(serverIndex.compareTo(startIndex) > 0 && serverIndex.compareTo(endIndex) > 0)
			{
				movingData = this.serverInstance.getKvdata().findMovingData(startIndex,serverIndex,true);
			}
			else
			{
				movingData = this.serverInstance.getKvdata().findMovingData(startIndex,endIndex,true);
			}
		}
		else
		{
			if(serverIndex.compareTo(startIndex) > 0 && serverIndex.compareTo(endIndex) < 0)
			{
				movingData = this.serverInstance.getKvdata().findMovingData(startIndex,serverIndex,false);
			}
			else
			{
				movingData = this.serverInstance.getKvdata().findMovingData(startIndex,endIndex,false);
			}
		}
		String move = null;
		//logger.info("dataserver:"+this.serverInstance.getPort()+"startindex:" + startIndex + "endindex:" + endIndex);
		ECSMessage sendMessage = new ECSMessage(ECSStatusType.MOVE_DATA_INTERNAL,movingData);

		KVCommunication communication = new KVCommunication(serverData.getAddress(), serverData.getPort(), "ECS");
		communication.sendMessage(sendMessage.toBytes());
		byte[] receivedmessage = communication.receiveMessage();
		ECSMessage receiveMessage = new ECSMessage(receivedmessage);
		if(receiveMessage.getCommand().equals(ECSStatusType.MOVE_DATA_INTERNAL_SUCCESS))
		{
			move = "movecompleted";
		}
		this.serverInstance.getKvdata().remove(movingData);
		logger.info("data removed");
		//this.serverInstance.getMovedDataList().add(movingData);
		// need to send message 
		return move;

	}
	/*private void removeData(KVServer kvserver)
	{
		if(this.serverInstance.getMovedDataList() != null)
		{
		for(HashMap<BigInteger,String> movedData : this.serverInstance.getMovedDataList())
		{
			try
			{
			this.serverInstance.getKvdata().remove(movedData);
			}
			catch(Exception e)
			{
				logger.error("Error while removing data" + e.getMessage());
			}
		}
		}
	}*/
}
