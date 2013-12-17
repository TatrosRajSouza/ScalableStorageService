package app_kvServer;

import java.io.IOException;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.HashMap;

import org.apache.log4j.Logger;

import client.KVCommunication;
import common.messages.ECSMessage;
import common.messages.ECSStatusType;
import common.messages.InfrastructureMetadata;
import common.messages.InvalidMessageException;
import common.messages.ServerData;

public class EcsConnection {
	private static Logger logger = Logger.getRootLogger();
	private String move;
	private ECSMessage ecsMessage;
	private KVServer serverInstance;
	public EcsConnection(byte[] latestMsg,KVServer serverInstance) throws InvalidMessageException {
		// TODO Auto-generated constructor stub
		this.ecsMessage = new ECSMessage(latestMsg);
		this.serverInstance = serverInstance;
	}
	public String process() throws InvalidMessageException {
		// TODO Auto-generated method stub
if(ecsMessage.getCommand() == ECSStatusType.INIT)
	initKVServer(ecsMessage.getMetadata());
else if(ecsMessage.getCommand() == ECSStatusType.START)
	start();
else if(ecsMessage.getCommand() == ECSStatusType.STOP)
	stopServer();
else if(ecsMessage.getCommand() == ECSStatusType.SHUTDOWN)
shutDown();
else if(ecsMessage.getCommand() == ECSStatusType.LOCK_WRITE)
	lockWrite();
else if(ecsMessage.getCommand() == ECSStatusType.UNLOCK_WRITE)
UnLockWrite();
else if(ecsMessage.getCommand() == ECSStatusType.UPDATE)
update(ecsMessage.getMetadata().toString());
else if(ecsMessage.getCommand() == ECSStatusType.MOVE_DATA_INTERNAL)
{
	moveData(ecsMessage.getMovingData());
}
else if(ecsMessage.getCommand() == ECSStatusType.MOVE_DATA)
{
 try {
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
else if(ecsMessage.getCommand().equals(ECSStatusType.MOVE_DATA_INTERNAL))
{
	this.serverInstance.getKvdata().moveData(ecsMessage.getMovingData());
	move = "moveinternalcompleted";
	
}
return move;
	}
	private void moveData(HashMap<BigInteger, String> movingData) {
		// TODO Auto-generated method stub
		this.serverInstance.getKvdata().moveData(movingData);
	}
	/**
	 * Initializes and starts the server but blocked for client requests
	 */
	// convert with meta data parameter
	private void initKVServer(InfrastructureMetadata metaData)
	{
		this.serverInstance.setMetaData(metaData);
	}
	private  void start()
	{

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
			System.exit(1);
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

			KVCommunication communication = new KVCommunication(serverData.getAddress(), serverData.getPort());
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
		for(HashMap<BigInteger,String> movedData : this.serverInstance.getMovedDataList())
		{
			this.serverInstance.getKvdata().remove(movedData);
		}
	}
}
