package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.util.List;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import crypto_protocol.HandshakeException;
import crypto_protocol.Message;
import crypto_protocol.MessageType;
import crypto_protocol.ClientInitMessage;
import crypto_protocol.ClientKeyExchangeMessage;
import crypto_protocol.SessionException;
import common.CommonCrypto;
import common.Ident;
import crypto_protocol.ErrorMessage;
import crypto_protocol.ServerAuthConfirmationMessage;
import common.ServerData;
import crypto_protocol.ServerInitMessage;
import crypto_protocol.SessionInfo;
import common.Settings;
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
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private boolean isOpen;
	private SessionInfo session;
	private boolean handshakeComplete = false;
	private Ident partner;

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

		LogSetup ls = new LogSetup("logs/server.log", "Server", Level.ALL);
		this.logger = ls.getLogger();
	}

	/**
	 * General receive method
	 * @return Message received Message
	 * @throws IOException
	 */
	public Message bytesToMessage(byte[] bytes) throws IOException {

		try {
			Object input =  CommonCrypto.objectFromByteArray(bytes);

			if (input instanceof ClientInitMessage) {
				logger.info("RECEIVE << " + input);
				return (ClientInitMessage)input;

			} else if (input instanceof ClientKeyExchangeMessage) {
				logger.info("RECEIVE << " + input);
				return (ClientKeyExchangeMessage)input;

			} else if (input instanceof ErrorMessage) {
				logger.info("RECEIVE << " + input);
				return (ErrorMessage)input;

			} else if (input instanceof ServerAuthConfirmationMessage) {
				logger.info("RECEIVE << " + input);
				return (ServerAuthConfirmationMessage)input;

			} else if (input instanceof ServerInitMessage) {
				logger.info("RECEIVE << " + input);
				return (ServerInitMessage)input;
			} else {
				throw new IOException("Received Object is not a valid Message. Expected <Message>, Received <" + input.getClass() + ">");
			}
		} catch (ClassNotFoundException e) {
			throw new IOException("Received Object was not of the expected type. Expected <Message>, Received <UnknownType>");
		}
	}

	/**
	 * Verifies that a given message is of a specific type
	 * @param message message to verify
	 * @param expectedType the expected MessageType
	 * @return true if message is of specified type, false otherwise
	 * @throws HandshakeException 
	 * @throws InvalidMessageException 
	 * @throws Exception in case of an error
	 */
	public boolean verifyMessageType(Message message, MessageType expectedType) throws HandshakeException {
		if (message.getType().equals(expectedType)) {
			return true;
		} else if (message.getType().equals("ErrorMessage")) {
			ErrorMessage errorMessage = (ErrorMessage) message;
			throw new HandshakeException("Received Error Message from Client: " + errorMessage.getMessage());
		} else {
			throw new HandshakeException("Received unexpected Message from Client. Type: " + message.getType());
		}
	}

	/**
	 * Send java object over socket output stream
	 * @param obj a java object
	 * @throws IOException
	 */
	public void sendObject(Object obj) throws IOException {
		if (obj == null) {
			throw new IOException("Tried to send null Object.");
		}

		logger.info("SEND >> " + obj.toString());
		byte[] bytes = CommonCrypto.objectToByteArray(obj);
		sendMessage(bytes);
	}

	private void performHandshake(Socket clientSocket, byte[] firstMessage) throws HandshakeException, IOException, SessionException {
		session.setServerIP(clientSocket.getLocalAddress().getHostAddress());
		session.setClientIP(clientSocket.getInetAddress().getHostAddress());
		session.setLocalPort(clientSocket.getLocalPort());
		session.setRemotePort(clientSocket.getPort());

		/* Try to import CA Certificate */
		try {
			session.setCACertificate(CommonCrypto.importCACertificate(Settings.getCACertPath()));
		} catch (CertificateException e) {
			logger.error("Unable to import CA Certificate from file: " + Settings.getCACertPath() + ", or Certificate invalid.\nReason: " + e.getMessage() + "\nClient Application Exiting.");
			System.exit(1);
		}

		// Expect ClientInit
		Message message = bytesToMessage(firstMessage);
		if (!verifyMessageType(message, MessageType.ClientInitMessage))
			throw new HandshakeException("Invalid Message received. Expected ClientInitMessage.");

		ClientInitMessage clientInitMessage = (ClientInitMessage) message;
		session.setClientNonce(clientInitMessage.getNonce());	

		// Send ServerInit
		ServerInitMessage serverInitMessage = new ServerInitMessage(Settings.TRANSFER_ENCRYPTION, KVServer.serverCertificate, session.isClientAuthRequired());
		session.setServerNonce(serverInitMessage.getNonce());
		session.setServerCertificate(serverInitMessage.getCertificate());
		sendObject(serverInitMessage);


		// Expect ClientKeyExchangeMessage
		message = bytesToMessage(receiveMessage(session.getEncKey(), session.getIV()));
		if (!verifyMessageType(message, MessageType.ClientKeyExchangeMessage))
			throw new HandshakeException("Invalid Message received.");

		ClientKeyExchangeMessage clientKeyExchangeMessage = (ClientKeyExchangeMessage) message;
		session.setClientCertificate(clientKeyExchangeMessage.getClientCertificate());
		session.setEncryptedSecret(clientKeyExchangeMessage.getEncryptedSecret());	

		// Decrypt the master secret
		session.setMasterSecret(CommonCrypto.decryptRSA(clientKeyExchangeMessage.getEncryptedSecret(), Settings.ALGORITHM_ENCRYPTION, KVServer.getPrivateKey()));
		logger.debug("RECEIVED & DECRYPTED MASTER SECRET (p): " + new String(session.getMasterSecret(), Settings.CHARSET));

		// Generate session keys for encryption and mac from received master secret
		try {
			session.setEncKey(CommonCrypto.generateSessionKey(Settings.ALGORITHM_HASHING, session.getMasterSecret(), session.getClientNonce(), session.getServerNonce(), new String("00000000").getBytes(Settings.CHARSET)));
			session.setMacKey(CommonCrypto.generateSessionKey(Settings.ALGORITHM_HASHING, session.getMasterSecret(), session.getClientNonce(), session.getServerNonce(), new String("11111111").getBytes(Settings.CHARSET)));
		} catch (InvalidKeyException e) {
			throw new HandshakeException ("Unable to generate Session keys, Invalid key: " + Settings.ALGORITHM_ENCRYPTION + ",\nMessage: " + e.getMessage()+ "\nConnection terminated.");
		} catch (NoSuchAlgorithmException e) {
			throw new HandshakeException ("Unable to generate Session keys, Invalid cipher: " + Settings.ALGORITHM_ENCRYPTION + ",\nMessage: " + e.getMessage()+ "\nConnection terminated.");
		}

		// Authenticate Client
		if (session.isClientAuthRequired()) {
			try {
				CommonCrypto.verifyCertificate(session.getClientCertificate(), session.getCACertificate());
			} catch (InvalidKeyException e1) {
				throw new HandshakeException(e1.getMessage());
			} catch (CertificateException e1) {
				throw new HandshakeException(e1.getMessage());
			} catch (NoSuchAlgorithmException e1) {
				throw new HandshakeException(e1.getMessage());
			} catch (NoSuchProviderException e1) {
				throw new HandshakeException(e1.getMessage());
			} catch (SignatureException e1) {
				throw new HandshakeException(e1.getMessage());
			}
			System.out.println(session.getClientCertificate());
			logger.info("Client Certificate verified by CA.");

			try {
				byte[] decryptedSignature = CommonCrypto.decryptRSA(clientKeyExchangeMessage.getSignature(), Settings.ALGORITHM_ENCRYPTION, session.getClientCertificate().getPublicKey());
				byte[] sigContent = CommonCrypto.concatenateByteArray(session.getServerNonce(), session.getEncryptedSecret());
				byte[] sigContentHash = CommonCrypto.generateHash(Settings.ALGORITHM_HASHING, session.getMacKey(), sigContent);



				if (CommonCrypto.isByteArrayEqual(sigContentHash, decryptedSignature)) {
					logger.info("Client Signature verified on Server Nonce and encrypted master secret.");
				} else {
					throw new SignatureException("Client Signature invalid. Signature hash did not match.");
				}
			} catch (Exception e) {
				//logger.error("Client Signature invalid or unable to verify Signature. Message: " + e.getMessage() + "\nSession terminated.");
				throw new HandshakeException("Client Signature invalid or unable to verify Signature. Message: " + e.getMessage() + "\nSession terminated.");
			}
		}

		// Compute session hash from session information
		try {
			session.setSecureSessionHash(CommonCrypto.generateSessionHash(Settings.ALGORITHM_HASHING, session.getMacKey(), session.getClientNonce(), session.getServerNonce(), session.getServerCertificate().getEncoded(), session.isClientAuthRequired()));
		} catch (InvalidKeyException e) {
			throw new HandshakeException ("Unable to generate Session Hash, Invalid key: " +  Settings.ALGORITHM_HASHING + ",\nMessage: " + e.getMessage()+ "\nConnection terminated.");
		} catch (CertificateEncodingException e) {
			throw new HandshakeException ("Unable to generate Session Hash, Invalid Certificate Encoding: " + Settings.ALGORITHM_HASHING + ",\nMessage: " + e.getMessage()+ "\nConnection terminated.");
		} catch (NoSuchAlgorithmException e) {
			throw new HandshakeException ("Unable to generate Session Hash, Invalid Algorithm: " + Settings.ALGORITHM_HASHING + ",\nMessage: " + e.getMessage()+ "\nConnection terminated.");
		}

		// Compare generated session hash with session hash received from client
		if (!(CommonCrypto.isByteArrayEqual(session.getSecureSessionHash(), clientKeyExchangeMessage.getSecureSessionHash()))) {
			throw new HandshakeException("Session Information Mismatch. Session Hash received from Client did not match Session Hash computed on Server.");
		} else {
			logger.info("Successfully compared Session Information. Session Hash on Server matches Hash received from Client.");
		}

		// Generate Auth Confirmation Hash & send to client
		try {
			if (session.isClientAuthRequired())
				session.setSecureConfirmationHash(CommonCrypto.generateConfirmationHash(Settings.ALGORITHM_HASHING, session.getMacKey(), session.getEncryptedSecret(), session.getSecureSessionHash(), session.getClientCertificate()));
			else
				session.setSecureConfirmationHash(CommonCrypto.generateConfirmationHash(Settings.ALGORITHM_HASHING, session.getMacKey(), session.getEncryptedSecret(), session.getSecureSessionHash()));
		} catch (Exception e) {
			throw new HandshakeException(e.getMessage());
		}
		ServerAuthConfirmationMessage serverAuthConfirmationMessage = new ServerAuthConfirmationMessage(session.getSecureConfirmationHash(), session.getIV(), session.isClientAuthRequired());
		sendObject (serverAuthConfirmationMessage);

		logger.debug(session);

		// Validate Session
		try {
			session.validateSession();
		} catch (SessionException e) {
			throw new HandshakeException(e.getMessage());
		}

		logger.info("Session validated. Handshake is complete.");
		this.handshakeComplete = true;
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

				try {
					session = new SessionInfo(clientSocket.getLocalAddress().getHostAddress() + ":" + clientSocket.getLocalPort(), Settings.TRANSFER_ENCRYPTION);
					session.setClientAuthRequired(false);
				} catch (SessionException e2) {
					throw new IOException("Unable to create session:\n" + e2.getMessage());
				}


				while(isOpen) { // until connection open
					try { //connection lost
						byte[] latestMsg = receiveMessage(session.getEncKey(), session.getIV());
						logger.debug("Received from     [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " + "RAW DATA\n<" + new String(latestMsg, Settings.CHARSET) + ">");

						if (!handshakeComplete && partner.equals(Ident.CLIENT)) {
							try {
								performHandshake(clientSocket, latestMsg);
							} catch (HandshakeException ex) {
								// TODO Auto-generated catch block
								logger.error("Error during secure handshake:\n" + ex.getMessage());
								ex.printStackTrace();
							} catch (IOException ex) {
								logger.error("Error during secure handshake:\n" + ex.getMessage());
								ex.printStackTrace();
							} catch (SessionException ex) {
								logger.error("Error during secure handshake:\n" + ex.getMessage());
								ex.printStackTrace();
							}
						}

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
										sendMessageEncrypted(kvQueryGet.toBytes(), session);
										logger.debug("SERVER:Get success");
									}
									else if (checkRangeReplicas(key)) 
									{
										KVQuery kvQueryGetError = new KVQuery(KVMessage.StatusType.GET_ERROR, key);
										logger.debug("Sent to           [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " 
												+ kvQueryGetError.getStatus() + " <" + kvQueryGetError.getKey() + ">");
										sendMessageEncrypted(kvQueryGetError.toBytes(), session);
									}
									else
									{
										// send not responsible message with metadata
										KVQuery kvQueryNotResponsible = new KVQuery(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE,"metaData",this.serverInstance.getMetaData().toString());
										logger.debug("Sent to           [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " 
												+ kvQueryNotResponsible.getStatus() + " <" + kvQueryNotResponsible.getKey() + ", " + kvQueryNotResponsible.getValue() + ">");
										sendMessageEncrypted(kvQueryNotResponsible.toBytes(), session);
									}

								}// get block
								else if(command.equals("PUT")) //put block
								{
									key = kvQueryCommand.getKey();
									value = kvQueryCommand.getValue();

									boolean isInRange = checkRangeCoordinator(key, value);
									BigInteger hashedKey = ConsistentHashing.hashKey(key);
									if(isInRange)
									{
										//future : check in range or not
										if(!this.serverInstance.isWriteLocked())
										{
											if(this.serverInstance.isDEBUG())
											{
												logger.info("SERVER: Put operation Key: " + key + " and Value: " + value);
											}
											returnValue = this.serverInstance.getKvdata().put(hashedKey,value);
											if(!value.equals("null") )
											{
												if(returnValue == null)
												{
													logger.debug("value: " + value + ", returnValue: " + returnValue + " --> PUT_SUCCESS");
													sendServerServerPut(key, value);
													KVQuery kvQueryPut = new KVQuery(KVMessage.StatusType.PUT_SUCCESS,key,value);
													sendMessageEncrypted(kvQueryPut.toBytes(), session);
													logger.debug("Sent to           [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " 
															+ kvQueryPut.getStatus() + " <" + kvQueryPut.getKey() + ", " + kvQueryPut.getValue() + ">");

												}
												else if(returnValue == value)
												{
													logger.debug("value: " + value + ", returnValue: " + returnValue + " --> PUT_UPDATE");
													sendServerServerPut(key, value);
													KVQuery kvQueryUpdate = new KVQuery(KVMessage.StatusType.PUT_UPDATE,key,value);
													sendMessageEncrypted(kvQueryUpdate.toBytes(), session);
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
													sendMessageEncrypted(kvQueryDelete.toBytes(), session);
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
										sendMessageEncrypted(kvQueryNotResponsible.toBytes(), session);
									}
								}//put block

								else if(command.equals("DISCONNECT")) //disconnect block
								{
									KVQuery kvQueryDisconnect;
									try {
										kvQueryDisconnect = new KVQuery(KVMessage.StatusType.DISCONNECT_SUCCESS);
										sendMessageEncrypted(kvQueryDisconnect.toBytes(), session);
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

								}// connect block only for clients not for ECS

							} // ECS permission to serve clients?
							else if(command.equals("CONNECT")) // connect block only for clients not for ECS
							{
								sendConnectSuccess(connectSuccess);

							} // connect block only for clients not for ECS
							else // server stopped block
							{
								KVQuery kvQueryNoService;
								try {
									kvQueryNoService = new KVQuery(KVMessage.StatusType.SERVER_STOPPED,key,value);
									sendMessageEncrypted(kvQueryNoService.toBytes(), session);
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
									}
									else if(ecsMessage.equals("moveinternalcompleted"))
									{
										ECSMessage ecsMoveSuccess = new ECSMessage(ECSStatusType.MOVE_DATA_INTERNAL_SUCCESS);
										sendMessage(ecsMoveSuccess.toBytes());
									}
									else
									{
										ECSMessage ecsMoveSuccess = new ECSMessage(ECSStatusType.MOVE_ERROR);
										sendMessage(ecsMoveSuccess.toBytes());
									}
								}
							} catch (InvalidMessageException eEcs) {//Server-server message
								try {
									ServerConnection serverConnection = new ServerConnection(latestMsg, this.serverInstance);
									serverConnection.process();
								}
								catch (InvalidMessageException eServer) {
									logger.error("Invalid message received from ECS");
								}	
							} 
						}//ECS block
					}//connection lost
					catch (IOException ioe) {
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
			if (serverInstance.getLastNodeData() != null)
				value = serverInstance.getLastNodeData().get(hashedKey);
			if (value == null) {
				if (serverInstance.getLastLastNodeData() != null)
					value = serverInstance.getLastLastNodeData().get(hashedKey);
			}
		}
		return value;
	}

	private void sendServerServerPut(String key, String value) throws SocketTimeoutException, IOException {
		ServerServerMessage serverServerMessage;
		try {
			if (serverInstance.getNextServer() != null) {
				serverServerMessage = new ServerServerMessage(ServerServerStatustype.SERVER_PUT,
						1, key, value);
				serverInstance.getNextServer().sendMessage(serverServerMessage.toBytes());
			}
		} catch (InvalidMessageException e) {
			logger.error("Error while updation"+e.getMessage());
			e.printStackTrace();
		}
		try {
			if (serverInstance.getNextNextServer() != null) {
				serverServerMessage = new ServerServerMessage(ServerServerStatustype.SERVER_PUT,
						2, key, value);
				serverInstance.getNextNextServer().sendMessage(serverServerMessage.toBytes());
			}
		} catch (InvalidMessageException e) {
			logger.error("Error while updation"+e.getMessage());
			e.printStackTrace();
		}
	}

	private void sendServerServerDelete(String key) throws SocketTimeoutException, IOException {
		ServerServerMessage serverServerMessage;
		try {
			if (serverInstance.getNextServer() != null) {
				serverServerMessage = new ServerServerMessage(ServerServerStatustype.SERVER_DELETE,
						1, key);
				serverInstance.getNextServer().sendMessage(serverServerMessage.toBytes());
			}
		} catch (InvalidMessageException e) {
			logger.error("Error while updation" + e.getMessage());
		}
		try {
			if (serverInstance.getNextNextServer() != null) {
				serverServerMessage = new ServerServerMessage(ServerServerStatustype.SERVER_DELETE,
						2, key);
				serverInstance.getNextNextServer().sendMessage(serverServerMessage.toBytes());
			}
		} catch (InvalidMessageException e) {
			logger.error("Error while updation" + e.getMessage());
		}
	}

	private void sendConnectSuccess(String connectSuccess) {
		KVQuery kvQueryConnect;
		try {
			kvQueryConnect = new KVQuery(KVMessage.StatusType.CONNECT_SUCCESS,connectSuccess );

			//sendMessage(kvQueryConnect.toBytes());
			sendMessageEncrypted(kvQueryConnect.toBytes(), session);

			if (kvQueryConnect.getStatus() != null)
				logger.debug("Sent to           [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " + kvQueryConnect.getStatus());
		} catch (InvalidMessageException e) {
			logger.error("Invalid connect message");
		} catch (IOException e) {
			logger.error("Error in sending connect success: " + e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * @param key
	 * @param value 
	 * @return hashedkey
	 * checks whether this server is the coordinator for incoming key
	 */
	private boolean checkRangeCoordinator(String key, String value) {
		try {

			ServerData serverDataHash = this.serverInstance.getConsistentHashing().getServerForKey(key);
			ServerData serverDataServer = this.serverInstance.getServerData();
			logger.info("Checking range for <key, value>: <" + key + ", " + value + ">");
			logger.info("Check range returned address: " + serverDataHash.getAddress().toString());
			logger.info("Check range returned port: " + serverDataHash.getPort());


			if (serverDataHash != null && serverDataServer != null)
			{
				if(serverDataHash.equals(serverDataServer))
				{
					logger.info("Server is responsible for this key.");
					return true;
				} 
			}
		} catch (IllegalArgumentException e) {
			logger.error("Illegal key" + e.getMessage());
		} catch (EmptyServerDataException e) {
			logger.error("no servers in the circle" + e.getMessage());

		} catch(Exception e) {
			logger.error("unexpected exception in check range" + e.getMessage());
		}
		return false;
	}

	private boolean checkRangeReplicas(String key) {
		List<ServerData> servers;
		try {
			ServerData serverInstanceData = serverInstance.getServerData();
			servers = serverInstance.getConsistentHashing().getServersForKey(key);
			for (ServerData server : servers) {
				if (server.getPort() == serverInstanceData.getPort() && server.getAddress().equals(serverInstanceData.getAddress())) {
					return true;
				}
			}
		} catch (EmptyServerDataException e) {
			logger.warn(e.getMessage());
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

	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessageEncrypted(byte[] msgBytes, SessionInfo session) throws IOException {
		sendMessageEncrypted(msgBytes, output, session);
	}

	/**
	 * Method sends a Message using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	private void sendMessage(byte[] msgBytes, OutputStream output) throws IOException, SocketTimeoutException {
		if (msgBytes != null) {

			byte[] bytes = ByteBuffer.allocate(12 + msgBytes.length).putInt(0).putInt(2).putInt(msgBytes.length).put(msgBytes).array();
			output.write(bytes, 0, bytes.length);
			output.flush();

			logger.debug("Sent to           [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " + new String(bytes, Settings.CHARSET));
		} else {
			throw new IOException("Unable to transmit message, the message was null.");
		}
	}

	/**
	 * Method sends a Message using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	private void sendMessageEncrypted(byte[] msgBytes, OutputStream output, SessionInfo session) throws IOException, SocketTimeoutException {
		if (msgBytes != null) {

			byte[] bytes = null;
			byte[] encryptedBytes = null;
			try {
				/* Encrypt contents AES-CBC-128 */	 
				SecretKeySpec k = new SecretKeySpec(session.getEncKey().getEncoded(), "AES");
				Cipher cipher = Cipher.getInstance(Settings.TRANSFER_ENCRYPTION);
				cipher.init (Cipher.ENCRYPT_MODE, k, new IvParameterSpec(session.getIV()));
				encryptedBytes = cipher.doFinal(msgBytes);
				bytes = ByteBuffer.allocate(12 + encryptedBytes.length).putInt(1).putInt(2).putInt(encryptedBytes.length).put(encryptedBytes).array();
			} catch (Exception e) {
				e.printStackTrace();
				throw new IOException("Unable to encrypt message: " + e.getMessage());
			}

			output.write(bytes, 0, bytes.length);
			output.flush();

			logger.debug("Sent to           [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " + new String(encryptedBytes, Settings.CHARSET));
		} else {
			throw new IOException("Unable to transmit message, the message was null.");
		}
	}

	public byte[] receiveMessage(Key decryptionKey, byte[] IV) throws IOException, SocketTimeoutException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read message encryption flag */
		byte[] encFlagBytes = new byte[4];
		input.read(encFlagBytes);
		int encFlag = ByteBuffer.wrap(encFlagBytes).getInt(); // 0 = Plain, 1 = Encrypted

		if (encFlag != 0 && encFlag != 1)
			throw new IOException("Encryption flag of received message was set to invalid value");

		/* read message identity */
		byte[] identBytes = new byte[4];
		input.read(identBytes);
		int ident = ByteBuffer.wrap(identBytes).getInt(); // 1 = CLIENT, 2 = SERVER, 3 = ECS

		if (ident == 1) {
			partner = Ident.CLIENT;
			logger.debug("RECEIVED MESSAGE IS FROM CLIENT");
		} else if (ident == 2) {
			partner = Ident.SERVER;
			logger.debug("RECEIVED MESSAGE IS FROM SERVER");
		} else if (ident == 3) {
			partner = Ident.ECS;
			logger.debug("RECEIVED MESSAGE IS FROM ECS");
		} else {
			throw new IOException("Ident flag of received message was set to invalid value");
		}

		/* read length of message */
		byte[] lenBytes = new byte[4];
		input.read(lenBytes);
		int msgLen = ByteBuffer.wrap(lenBytes).getInt();

		if (msgLen < 0)
			throw new IOException("Length field of received message was invalid (negative).");

		logger.debug("new message - length: " + msgLen + ", ident: " + ident);

		for (int i = 0; i < msgLen; i++) {
			/* read next byte */
			byte read = (byte) input.read();

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

		if (encFlag == 0) {
			logger.debug("Received Plain from     [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " + "RAW DATA\n<" + new String(msgBytes, Settings.CHARSET) + ">");
			return msgBytes;
		} else {

			logger.debug("Received Encrypted from     [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " + "RAW DATA\n" + new String(msgBytes, Settings.CHARSET));
			if (decryptionKey == null)
				throw new IOException("Unable to decrypt message. No decryptionKey was supplied.");

			if (IV == null)
				throw new IOException("Unable to decrypt message. No IV was supplied.");

			/* Decrypt contents */
			try {
				byte[] plainBytes = CommonCrypto.decryptAES(msgBytes, Settings.TRANSFER_ENCRYPTION, decryptionKey, IV);
				logger.debug("Successfully decrypted message using " + Settings.TRANSFER_ENCRYPTION);
				logger.debug("---BEGIN DECRYPTED MESSAGE---");
				logger.debug(new String(plainBytes, Settings.CHARSET));
				logger.debug("---END DECRYPTED MESSAGE---");
				logger.debug("Decrypted Message from     [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " + "RAW DATA\n" + new String(plainBytes, Settings.CHARSET));
				return plainBytes;
			} catch (Exception e) {
				throw new IOException("Unable to decrypt message:\n" + e.getMessage());
			}
		}
	}
}