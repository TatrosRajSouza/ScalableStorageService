package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

import app_kvServer.KVData;
import common.messages.InvalidMessageException;
import common.messages.KVMessage;
import common.messages.KVQuery;
import consistent_hashing.ConsistentHashing;
/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the get,put functionality. 
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	private boolean serveClientRequest;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	public boolean isWriteLocked() {
		return isWriteLocked;
	}

	public void setWriteLocked(boolean isWriteLocked) {
		this.isWriteLocked = isWriteLocked;
	}


	private boolean isWriteLocked;
	private boolean isOpen;
	public boolean isServeClientRequest() {
		return serveClientRequest;
	}

	public void setServeClientRequest(boolean serveClientRequest) {
		this.serveClientRequest = serveClientRequest;
	}

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
	public ClientConnection(Socket clientSocket) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.serveClientRequest = false;
		this.isWriteLocked = false;
	}

	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */ // example usage for testing: connect 127.0.0.1 50001
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			String connectSuccess = "Connection to MSRG Echo server established: " 
					+ clientSocket.getLocalAddress() + " / "
					+ clientSocket.getLocalPort();
			//need to be edited
			KVQuery kvQueryConnect;
			try {
				kvQueryConnect = new KVQuery(KVMessage.StatusType.CONNECT_SUCCESS,connectSuccess );
				sendMessage(kvQueryConnect.toBytes());
			} catch (InvalidMessageException e) {
				// TODO Auto-generated catch block
				logger.error("Invalid connect message");
			}
			while(isOpen) {
				try {
					// Serve clients only after ECS permission
					if(serveClientRequest)
					{
						byte[] latestMsg = receiveMessage();
						KVQuery kvQueryCommand;
						try {
							kvQueryCommand = new KVQuery(latestMsg);
							String key=null,value=null,returnValue=null;
							String command = kvQueryCommand.getStatus().toString();
							logger.debug("Received Command is: " + command);

							if(command.equals("GET"))	{
								key = kvQueryCommand.getKey();
								int hashedKey = checkRange(key);
								if(hashedKey != 0)
								{
									//future : check in range or not
									System.out.println("trying to get key");

									System.out.println("Key is: " + kvQueryCommand.getKey());


									logger.debug("Num keys in map: " + KVServer.kvdata.dataStore.size());
									for (int k : KVServer.kvdata.dataStore.keySet())
									{
										logger.debug("Key: " + k);
									}

									returnValue = KVServer.kvdata.get(hashedKey);
									logger.debug("returnValue: " + returnValue);
									if(returnValue != null) {
										KVQuery kvQueryGet = new KVQuery(KVMessage.StatusType.GET_SUCCESS, returnValue);
										sendMessage(kvQueryGet.toBytes());
									}
									else
									{
										String errorMsg = "Error in get operation for the key/Key is not present <key>: " + "<" + key + ">";
										logger.error(errorMsg);
										KVQuery kvQueryGeterror = new KVQuery(KVMessage.StatusType.GET_ERROR,errorMsg);
										sendMessage(kvQueryGeterror.toBytes());
									}
								}
								else
								{
									// send not responsible message with metadata
								}

							}
							else if(command.equals("PUT"))
							{
								key = kvQueryCommand.getKey();
								int hashedKey = checkRange(key);
								if(hashedKey != 0)
								{
								//future : check in range or not
								if(!this.isWriteLocked)
								{
									
									value = kvQueryCommand.getValue();

									returnValue = KVServer.kvdata.put(hashedKey,value);
									if(!value.equals("null") )
									{
										if(returnValue == null)
										{
											KVQuery kvQueryPut = new KVQuery(KVMessage.StatusType.PUT_SUCCESS,key,value);
											sendMessage(kvQueryPut.toBytes());
										}
										else if(returnValue == value)
										{
											KVQuery kvQueryUpdate = new KVQuery(KVMessage.StatusType.PUT_UPDATE,key,value);
											sendMessage(kvQueryUpdate.toBytes());
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
											KVQuery kvQueryDelete = new KVQuery(KVMessage.StatusType.DELETE_SUCCESS,key,returnValue);
											sendMessage(kvQueryDelete.toBytes());
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
								// send not responsible error message and meta data	
								}
							}
							
							else if(command.equals("DISCONNECT"))
							{
								KVQuery kvQueryDisconnect;
								try {
									kvQueryDisconnect = new KVQuery(KVMessage.StatusType.DISCONNECT_SUCCESS);
									sendMessage(kvQueryDisconnect.toBytes());
								} catch (InvalidMessageException e) {
									// TODO Auto-generated catch block
									logger.error("Error in sending disconnect message");
								}
							}

						} catch (InvalidMessageException e) {
							logger.error("Invalid message received from client");
							KVQuery kvQueryInvalid;
							try {
								kvQueryInvalid = new KVQuery(KVMessage.StatusType.FAILED,"Invalid command");
								sendMessage(kvQueryInvalid.toBytes());
							} catch (InvalidMessageException e1) {
								// TODO Auto-generated catch block
								logger.error("Error in invalid message format:server side");
							}


						}
					}
					else
					{
						KVQuery kvQueryNoService;
						try {
							kvQueryNoService = new KVQuery(KVMessage.StatusType.SERVER_STOPPED,"Server is stopped");
							sendMessage(kvQueryNoService.toBytes());
						} catch (InvalidMessageException e1) {
							// TODO Auto-generated catch block
							logger.error("Error in invalid message format:server side:");
						}
					}

				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}

			}


		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);

		} finally {

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



	private int checkRange(String key) {
		// TODO Auto-generated method stub
		ConsistentHashing consistentHashing = new ConsistentHashing();
		int hashedKey = consistentHashing.hashCode();
		return hashedKey;
		//MessageDigest md;
		//String hashedKey = null;
		//try {
			//md = MessageDigest.getInstance("MD5");
			//md.update(key.getBytes());
			//hashedKey =  md.digest().toString();
			/*byte byteData[] = md.digest();
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < byteData.length; i++) {
				sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
			}*/
			// if not in the range return null
			
		//} 
		//catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			//logger.error("Error in MD5 generation"+ e.getMessage());
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
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ new String(msgBytes) +"'");
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

		logger.info("RECEIVE \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ new String(msgBytes) + "'");
		return msgBytes;
	}



}