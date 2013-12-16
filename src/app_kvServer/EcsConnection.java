package app_kvServer;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;

import org.apache.log4j.Logger;

import common.messages.ECSMessage;
import common.messages.ECSStatusType;
import common.messages.InfrastructureMetadata;
import common.messages.InvalidMessageException;
import common.messages.ServerData;

public class EcsConnection {
	private static Logger logger = Logger.getRootLogger();
	private boolean move = false;
	private ECSMessage ecsMessage;
	public EcsConnection(byte[] latestMsg) throws InvalidMessageException {
		// TODO Auto-generated constructor stub
		ecsMessage = new ECSMessage(latestMsg);
	}
	public boolean process() throws InvalidMessageException {
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
update(ecsMessage.getMetadata());
else if(ecsMessage.getCommand() == ECSStatusType.MOVE_DATA)
{
moveData(ecsMessage.getStartIndex(), ecsMessage.getEndIndex(), ecsMessage.getServer());

move = true;

}
return move;
	}
	/**
	 * Initializes and starts the server but blocked for client requests
	 */
	// convert with meta data parameter
	private void initKVServer(InfrastructureMetadata metaData)
	{
		KVServer.metaData = metaData;
	}
	private  void start()
	{

		logger.info("Allowing server to serve client requests");
		KVServer.serveClientRequest = true;

	}
	private void stopServer(){		
		logger.info("Stopping server to serve client requests");
		KVServer.serveClientRequest =  false;

	}
	private void lockWrite()
	{
		logger.info("locking server to write requests");
		KVServer.isWriteLocked = true;
	}
	private void UnLockWrite()
	{
		logger.info("unlocking server to write requests");
		KVServer.isWriteLocked = false;
	}

	private void shutDown(){
		KVServer.running = false;
		try {
			KVServer.serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + KVServer.port, e);
			System.exit(1);
		}

	}
	private void update(InfrastructureMetadata metaData)
	{
		KVServer.metaData = metaData;
	}
	private void moveData(BigInteger startIndex, BigInteger endIndex, ServerData serverData)
	{
		HashMap<BigInteger, String> movingData = KVServer.kvdata.findMovingData(startIndex, endIndex);
		KVServer.kvdata.moveData(movingData);
		KVServer.movedDataList.add(movingData);
		// need to send message 
	}
	private void removeData(KVServer kvserver)
	{
		for(HashMap<BigInteger,String> movedData : KVServer.movedDataList)
		{
			KVServer.kvdata.remove(movedData);
		}
	}
}
