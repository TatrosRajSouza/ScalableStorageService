package app_kvServer;

import java.io.IOException;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.HashMap;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.KVCommunication;
import common.messages.ECSMessage;
import common.messages.ECSStatusType;
import common.messages.InfrastructureMetadata;
import common.messages.InvalidMessageException;
import common.messages.ServerData;
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
		// TODO Auto-generated constructor stub
		this.ecsMessage = new ECSMessage(latestMsg);
		this.serverInstance = serverInstance;
		
		LogSetup ls = new LogSetup("logs\\server.log", "Server", Level.ALL);
		this.logger = ls.getLogger();
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
			update(ecsMessage.getMetadata().toString());
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
				// TODO Auto-generated catch block
				logger.error("Error while updation"+e.getMessage());
			} catch (InvalidMessageException e) {
				// TODO Auto-generated catch block
				logger.error("Error while updation"+e.getMessage());
			}
		}

		return move;
	}

	private String moveData(HashMap<BigInteger, String> movingData) {
		// TODO Auto-generated method stub
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
	private void update(String metaDataString)
	{
		this.serverInstance.getMetaData().update(metaDataString);
	}
	private String moveData(BigInteger startIndex, BigInteger endIndex, ServerData serverData) throws UnknownHostException, IOException, InvalidMessageException
	{
		HashMap<BigInteger, String> movingData = this.serverInstance.getKvdata().findMovingData(startIndex, endIndex);
		String move = null;
		ECSMessage sendMessage = new ECSMessage(ECSStatusType.MOVE_DATA_INTERNAL,movingData);

		KVCommunication communication = new KVCommunication(serverData.getAddress(), serverData.getPort(), "ECS");
		communication.sendMessage(sendMessage.toBytes());
		byte[] receivedmessage = communication.receiveMessage();
		ECSMessage receiveMessage = new ECSMessage(receivedmessage);
		if(receiveMessage.getCommand().equals(ECSStatusType.MOVE_DATA_INTERNAL_SUCCESS))
		{
			move = "movecompleted";
		}

		this.serverInstance.getMovedDataList().add(movingData);
		// need to send message 
		return move;

	}
	private void removeData(KVServer kvserver)
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
	}
}
