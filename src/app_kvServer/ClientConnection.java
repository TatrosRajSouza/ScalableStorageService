package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.Socket;
import java.net.SocketTimeoutException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.ServerData;
import common.messages.ECSMessage;
import common.messages.ECSStatusType;
import common.messages.InvalidMessageException;
import common.messages.KVMessage;
import common.messages.KVQuery;
import common.messages.ServerServerMessage;
import common.messages.ServerServerStatustype;
import consistent_hashing.ConsistentHashing;
import consistent_hashing.EmptyServerDataException;
/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * @author Udhayaraj Sivalingam 
 */
public class ClientConnection implements Runnable {
	private KVServer serverInstance;
	private Logger logger;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 44096 * BUFFER_SIZE;
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private boolean isOpen;

	public boolean isOpen() {
		return isOpen;
	}

	public void setOpen(boolean isOpen) {
		this.isOpen = isOpen;
	}


	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket,KVServer serverInstance) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.serverInstance = serverInstance;

		LogSetup ls = new LogSetup("logs\\server.log", "Server", Level.ALL);
		this.logger = ls.getLogger();
	}

	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */ // example usage for testing: connect 127.0.0.1 50001
	public void run() {
		if (!Thread.interrupted()) {
			Thread.currentThread().setName("SERVER " + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getLocalPort());
			try { //connection could not be established
				output = clientSocket.getOutputStream();
				input = clientSocket.getInputStream();
				String connectSuccess = "Connection to MSRG Echo server established: " 
						+ clientSocket.getLocalAddress() + " / "
						+ clientSocket.getLocalPort();		
				while(isOpen) { // until connection open
					try { //connection lost
						byte[] latestMsg = receiveMessage();
						//logger.info("############## Receving message from " + serverInstance.getName());
						//logger.debug("Received a new message");
						KVQuery kvQueryCommand;
						try { //   not KVMessage
							kvQueryCommand = new KVQuery(latestMsg);
							String key=null,value=null,returnValue=null;
							String command = kvQueryCommand.getStatus().toString();

							if (command != null && kvQueryCommand.getKey() != null && kvQueryCommand.getValue() != null)
								logger.debug("Received from     [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " + command + " <" + kvQueryCommand.getKey() + ", " + kvQueryCommand.getValue() + ">");
							else if (command != null && kvQueryCommand.getKey() != null)
								logger.debug("Received from     [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " + command + " <" + kvQueryCommand.getKey() + ">");
							else if (command != null)
								logger.debug("Received from     [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " + command);

							if(this.serverInstance.isServeClientRequest()) //  ECS permission to serve client?
							{
								if(command.equals("GET"))	{ //get block

									key = kvQueryCommand.getKey();
									//logger.info("Metadata of:" +this.serverInstance.getPort() + " metadata:" + this.serverInstance.getMetaData().toString());
									if(this.serverInstance.isDEBUG())
									{
										logger.info("SERVER: Get operation Key:" + key);
									}

									returnValue = getValue(key);
									if(returnValue != null)
									{
										if(this.serverInstance.isDEBUG())
										{
											logger.debug("Num keys in map: " + this.serverInstance.getKvdata().dataStore.size());
											for (BigInteger k : this.serverInstance.getKvdata().dataStore.keySet())
											{
												logger.debug("Key: " + k);
											}
										}
										//logger.debug("returnValue: " + returnValue);
										KVQuery kvQueryGet = new KVQuery(KVMessage.StatusType.GET_SUCCESS, returnValue);
										sendMessage(kvQueryGet.toBytes());
										logger.debug("SERVER:Get success");
									}
									else
									{
										// send not responsible message with metadata
										KVQuery kvQueryNotResponsible = new KVQuery(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE,"metaData",this.serverInstance.getMetaData().toString());
										logger.debug("Sent to           [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " 
												+ kvQueryNotResponsible.getStatus() + " <" + kvQueryNotResponsible.getKey() + ", " + kvQueryNotResponsible.getValue() + ">");
										sendMessage(kvQueryNotResponsible.toBytes());
									}

								}// get block
								else if(command.equals("PUT")) //put block
								{
									key = kvQueryCommand.getKey();

									boolean isInRange = checkRangeCoordinator(key, value);
									BigInteger hashedKey = ConsistentHashing.hashKey(key);
									if(isInRange)
									{
										//future : check in range or not
										if(!this.serverInstance.isWriteLocked())
										{

											value = kvQueryCommand.getValue();
											if(this.serverInstance.isDEBUG())
											{
												logger.info("SERVER: Put operation Key: " + key + " and Value: " + value);
											}
											returnValue = this.serverInstance.getKvdata().put(hashedKey,value);
											if(!value.equals("null") )
											{
												if(returnValue == null)
												{
													sendServerServerPut(key, value);
													KVQuery kvQueryPut = new KVQuery(KVMessage.StatusType.PUT_SUCCESS,key,value);
													sendMessage(kvQueryPut.toBytes());
													logger.debug("Sent to           [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " 
															+ kvQueryPut.getStatus() + " <" + kvQueryPut.getKey() + ", " + kvQueryPut.getValue() + ">");

												}
												else if(returnValue == value)
												{
													sendServerServerPut(key, value);
													KVQuery kvQueryUpdate = new KVQuery(KVMessage.StatusType.PUT_UPDATE,key,value);
													sendMessage(kvQueryUpdate.toBytes());
													logger.debug("SERVER:put update success");

												}
												else
												{
													String errorMsg = "Error in put operation for Key:"+key + "and value:" + value ;
													logger.error(errorMsg);
													sendError(KVMessage.StatusType.PUT_ERROR,key,value);
												}
											}

											if(value.equals("null"))
											{
												if(returnValue != null)
												{
													sendServerServerDelete(key);
													KVQuery kvQueryDelete = new KVQuery(KVMessage.StatusType.DELETE_SUCCESS,key,returnValue);
													sendMessage(kvQueryDelete.toBytes());
													logger.debug("SERVER:put delete success");

												}
												else
												{
													String errorMsg = "Error in Delete operation for Key:"+key  ;
													logger.error(errorMsg);
													sendError(KVMessage.StatusType.DELETE_ERROR,key,value);
												}

											}
										}
										else
										{
											KVQuery kvQueryWriteLock = new KVQuery(KVMessage.StatusType.SERVER_WRITE_LOCK);
											sendMessage(kvQueryWriteLock.toBytes());
										}
									}
									else
									{
										KVQuery kvQueryNotResponsible = new KVQuery(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE,"metaData",this.serverInstance.getMetaData().toString());
										logger.debug("Sent to           [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " 
												+ kvQueryNotResponsible.getStatus() + " <" + kvQueryNotResponsible.getKey() + ", " + kvQueryNotResponsible.getValue() + ">");
										sendMessage(kvQueryNotResponsible.toBytes());
									}
								}//put block

								else if(command.equals("DISCONNECT")) //disconnect block
								{
									KVQuery kvQueryDisconnect;
									try {
										kvQueryDisconnect = new KVQuery(KVMessage.StatusType.DISCONNECT_SUCCESS);
										sendMessage(kvQueryDisconnect.toBytes());
										logger.debug("Sent to           [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " 
												+ kvQueryDisconnect.getStatus());

										try {
											if (clientSocket != null) {
												input.close();
												output.close();
												clientSocket.close();
											}
										} catch (IOException ioe) {
											logger.error("Error! Unable to tear down connection!", ioe);
										}
									} catch (InvalidMessageException e) {
										logger.error("Error in sending disconnect message");
									}
								} //disconnect block
								else if(command.equals("CONNECT")) // connect block only for clients not for ECS
								{
									sendConnectSuccess(connectSuccess);
									// logger.debug("SERVER:Connect success");


								}// connect block only for clients not for ECS

							} // ECS permission to serve clients?
							else if(command.equals("CONNECT")) // connect block only for clients not for ECS
							{
								sendConnectSuccess(connectSuccess);
								logger.debug("SERVER:Connect success");


							} // connect block only for clients not for ECS
							else // server stopped block
							{
								KVQuery kvQueryNoService;
								try {
									kvQueryNoService = new KVQuery(KVMessage.StatusType.SERVER_STOPPED,key,value);
									sendMessage(kvQueryNoService.toBytes());
									logger.debug("SERVER:Stopped");

								} catch (InvalidMessageException e1) {
									logger.error("Error in invalid message format:server side:");
								}
							} //server stopped block

						}//   not KVMessage
						catch (InvalidMessageException e) {//ECS block
							try{
								EcsConnection ecsConnection = new EcsConnection(latestMsg,this.serverInstance);
								String ecsMessage = ecsConnection.process();
								if(ecsMessage != null)
								{
									if(ecsMessage.equals("movecompleted"))
									{
										ECSMessage ecsMoveSuccess = new ECSMessage(ECSStatusType.MOVE_COMPLETED);
										sendMessage(ecsMoveSuccess.toBytes());
										logger.debug("SERVER:ECS move completed");
									}
									else if(ecsMessage.equals("moveinternalcompleted"))
									{
										ECSMessage ecsMoveSuccess = new ECSMessage(ECSStatusType.MOVE_DATA_INTERNAL_SUCCESS);
										sendMessage(ecsMoveSuccess.toBytes());
										logger.debug("SERVER:ECS movinternale completed");
									}
									else
									{
										ECSMessage ecsMoveSuccess = new ECSMessage(ECSStatusType.MOVE_ERROR);
										sendMessage(ecsMoveSuccess.toBytes());
										logger.debug("SERVER:ECS move error");
									}
								}
							}
							catch(InvalidMessageException eEcs)
							{
								try {
									//TODO
									logger.info("%%%%%%%%");
									ServerConnection serverConnection = new ServerConnection(latestMsg, this.serverInstance);
									ServerServerMessage serverMessage = serverConnection.process();
									logger.info("############## Receving message " + serverMessage.getCommand());


									/*String values = "&";
									for (BigInteger hash : serverInstance.getKvdata().dataStore.keySet()) {
										values += serverInstance.getKvdata().dataStore.get(hash);
									}
									logger.info("############## " + values);

									BigInteger hashedKey = ConsistentHashing.hashKey(serverMessage.getKey());
									serverInstance.getKvdata().put(hashedKey, serverMessage.getValue());*/

									/*if(serverMessage != null)
									{
										if(serverMessage.equals("movecompleted"))
										{
											ECSMessage ecsMoveSuccess = new ECSMessage(ECSStatusType.MOVE_COMPLETED);
											sendMessage(ecsMoveSuccess.toBytes());
											logger.debug("SERVER:ECS move completed");
										}
										else if(serverMessage.equals("moveinternalcompleted"))
										{
											ECSMessage ecsMoveSuccess = new ECSMessage(ECSStatusType.MOVE_DATA_INTERNAL_SUCCESS);
											sendMessage(ecsMoveSuccess.toBytes());
											logger.debug("SERVER:ECS movinternale completed");
										}
										else
										{
											ECSMessage ecsMoveSuccess = new ECSMessage(ECSStatusType.MOVE_ERROR);
											sendMessage(ecsMoveSuccess.toBytes());
											logger.debug("SERVER:ECS move error");
										}
									}*/
									/*ServerConnection serverConnection = new ServerConnection(latestMsg, this.serverInstance);
									String serverMessage = serverConnection.process();
									logger.info("############## Receving message from " + serverInstance.getName() + "\n" + serverMessage);*/
								}
								catch(InvalidMessageException eServer)
								{
									logger.error("Invalid message received from ECS");
								}	
							} 
						}//ECS block
					}//connection lost
					catch (IOException ioe) {
						// logger.error("Error! Connection lost!"); // I commented this out because it is irritating. Happens every disconnect.
						isOpen = false;
					}
				}// until connection open
			}//connection could not be established
			catch (IOException ioe) {
				logger.error("Error! Connection could not be established!", ioe);

			}
			finally {
				try {
					if (clientSocket != null) {
						input.close();
						output.close();
						clientSocket.close();
					}
				} catch (IOException ioe) {
					logger.error("Error! Unable to tear down connection!", ioe);
				}
			}
		}
	}

	private String getValue(String key) {
		BigInteger hashedKey = ConsistentHashing.hashKey(key);
		String value = serverInstance.getKvdata().get(hashedKey);
		if (value == null) {
			value = serverInstance.lastNodeData.get(hashedKey);
			if (value == null) {
				value = serverInstance.lastLastNodeData.get(hashedKey);
			}
		}
		return value;
	}

	private void sendServerServerPut(String key, String value) throws SocketTimeoutException, IOException {
		ServerServerMessage serverServerMessage = new ServerServerMessage(ServerServerStatustype.SERVER_PUT,
				1, key, value);
		serverInstance.nextServer.sendMessage(serverServerMessage.toBytes());
		serverServerMessage = new ServerServerMessage(ServerServerStatustype.SERVER_PUT,
				2, key, value);
		serverInstance.nextNextServer.sendMessage(serverServerMessage.toBytes());
	}

	private void sendServerServerDelete(String key) throws SocketTimeoutException, IOException {
		ServerServerMessage serverServerMessage = new ServerServerMessage(ServerServerStatustype.SERVER_DELETE,
				1, key);
		serverInstance.nextServer.sendMessage(serverServerMessage.toBytes());
		serverServerMessage = new ServerServerMessage(ServerServerStatustype.SERVER_DELETE,
				2, key);
		serverInstance.nextNextServer.sendMessage(serverServerMessage.toBytes());
	}

	private void sendConnectSuccess(String connectSuccess) {
		KVQuery kvQueryConnect;
		try {
			kvQueryConnect = new KVQuery(KVMessage.StatusType.CONNECT_SUCCESS,connectSuccess );

			if (kvQueryConnect.getStatus() != null)
				logger.debug("Sent to           [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " + kvQueryConnect.getStatus());

			sendMessage(kvQueryConnect.toBytes());
		} catch (InvalidMessageException e) {
			logger.error("Invalid connect message");
		} catch (IOException e) {
			logger.error("Error in sending connect success" + e.getMessage());;
		}
	}

	/***
	 * 
	 * @param key
	 * @param value 
	 * @return hashedkey
	 * checks whether this server is the coordinator for incoming key
	 */
	private boolean checkRangeCoordinator(String key, String value) {
		try {

			ServerData serverDataHash = this.serverInstance.getConsistentHashing().getServerForKey(key);
			ServerData serverDataServer = this.serverInstance.getServerData();
			logger.info("Check Range adress" + serverDataHash.getAddress().toString() + serverDataServer.getAddress().toString() );
			logger.info("Check Range port" + serverDataHash.getPort() + serverDataServer.getPort() );
			logger.info("key is: " + key + "value: " + value);
			if(this.serverInstance.isDEBUG())
			{
				logger.info("Check Range adress" + serverDataHash.getAddress().toString() + serverDataServer.getAddress().toString() );
				logger.info("Check Range port" + serverDataHash.getPort() + serverDataServer.getPort() );
				logger.info("key is: " + key + "value: " + value);
			}
			if (serverDataHash != null && serverDataServer != null)
			{
				if(serverDataHash.equals(serverDataServer))
				{
					logger.info("success");
					return true;
				} 
			}
		} catch (IllegalArgumentException e) {
			logger.error("Illegal key" + e.getMessage());
		} catch (EmptyServerDataException e) {
			logger.error("no servers in the circle" + e.getMessage());

		}
		catch(Exception e)
		{
			logger.error("unknown exception in check range" + e.getMessage());
		}
		return false;
	}



	private void sendError(KVMessage.StatusType statusType, String key, String value) throws UnsupportedEncodingException, IOException {
		KVQuery kvQueryError;
		try {
			kvQueryError = new KVQuery(statusType,key,value);
			sendMessage(kvQueryError.toBytes());
		} catch (InvalidMessageException e) {
			logger.error("Error in ErrorMessage format");
		}

	}

	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(byte[] msgBytes) throws IOException {
		sendMessage(msgBytes, output);
	}

	private void sendMessage(byte[] msgBytes, OutputStream output) throws IOException {
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		if(this.serverInstance.isDEBUG())
		{
			logger.info("SEND \t<" 
					+ clientSocket.getInetAddress().getHostAddress() + ":" 
					+ clientSocket.getPort() + ">: '" 
					+ new String(msgBytes) +"'");
		}
	}


	private byte[] receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;

		while(read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 

			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;

			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
		}

		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

		msgBytes = tmp;

		/* build final String */

		//if(this.serverInstance.isDEBUG())
		//{
		logger.info("RECEIVE @@@@@@\t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ new String(msgBytes) + "'");
		//}
		return msgBytes;
	}



}