package client;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.SignatureException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvClient.SocketStatus;
import crypto_protocol.ClientInitMessage;
import crypto_protocol.ClientKeyExchangeMessage;
import crypto_protocol.ErrorMessage;
import crypto_protocol.Message;
import crypto_protocol.MessageType;
import common.CommonCrypto;
import crypto_protocol.SessionInfo;
import crypto_protocol.HandshakeException;
import common.InfrastructureMetadata;
import crypto_protocol.ServerAuthConfirmationMessage;
import common.ServerData;
import crypto_protocol.ServerInitMessage;
import crypto_protocol.SessionException;
import common.Settings;
import common.messages.InvalidMessageException;
import common.messages.KVMessage;
import common.messages.KVQuery;
import common.messages.KVMessage.StatusType;
import consistent_hashing.ConsistentHashing;
import consistent_hashing.EmptyServerDataException;

/**
 * A library that enables any client application to communicate with a KVServer.
 * @author Elias Tatros
 *
 */
public class KVStore implements KVCommInterface {
	public static final boolean DEBUG = true;
	private KVCommunication kvComm;
	private Logger logger;
	private String address;
	private int port;
	private int currentRetries = 0;
	private static final int NUM_RETRIES = 3;
	private InfrastructureMetadata metaData;
	private ConsistentHashing consHash;
	private String name = "";
	private String moduleName = "<KVStore Module>";
	private Random generator = new Random();
	private SessionInfo session;
	private boolean handshakeComplete = false;
	private ArrayList<X509Certificate> trustedCAs;
	private PrivateKey clientPrivateKey;


	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port, String name) {
		this.address = address;
		this.port = port;
		this.name = name;

		initLog();

		/* Create empty meta data and add the user-specified server */
		this.metaData = new InfrastructureMetadata();
		this.metaData.addServer("Initial User-Specified Server", address, port);

		/* Initialize the consistent Hashing */
		this.consHash = new ConsistentHashing(metaData.getServers());
		
		/* 
		 * Try to import clients private key
		 * Please make sure key is in pkcs 8 format.
		 * For conversion you can use:
		 * "openssl pkcs8 -topk8 -nocrypt -inform PEM -outform DER -in inputKey.key.pem -out pkcs8OutputKey.key.pem"
		 */
		try {
			this.clientPrivateKey = CommonCrypto.loadPrivateKey(Settings.CLIENT_PRIVKEY_PATH, Charset.forName(Settings.CHARSET));
		} catch (UnsupportedCharsetException e) {
			logger.error("Cannot read Clients private key at: " + Settings.CLIENT_PRIVKEY_PATH + ",\nbecause the Charset " + Settings.CHARSET + " is not supported.\nClient Application terminated.");
			System.exit(1);
		} catch (FileNotFoundException e) {
			logger.error("Clients private key file not found at: " + Settings.CLIENT_PRIVKEY_PATH + "\nClient Application terminated.");
			System.exit(1);
		}catch (IOException e) {
			logger.error("Unable to read this clients private key from file: " + Settings.CLIENT_PRIVKEY_PATH +
					",\nReason: " + e.getMessage() + "\nClient Application terminated.");
			System.exit(1);
		} catch (InvalidKeySpecException e) {
			logger.error("Unable to read this clients private key from file: " + Settings.CLIENT_PRIVKEY_PATH +
					",\nReason: " + e.getMessage() + "\nPlease make sure the private key file is in PKCS8 DER Format.\n" +
							"To convert an unencrypted PEM key with openssl use the following command:\n" +
							"openssl pkcs8 -topk8 -nocrypt -inform PEM -outform DER -in inputKey.key.pem -out pkcs8OutputKey.key.pem\n" +
							"Client Application terminated.");
			System.exit(1);
		} catch (NoSuchAlgorithmException e) {
			logger.error("Unable to read this clients private key from file: " + Settings.CLIENT_PRIVKEY_PATH +
					",\nReason: " + e.getMessage() + "\nClient Application terminated.");
			System.exit(1);
		}
		
		/* Load trusted CAs from trust store */
		try {
			trustedCAs = CommonCrypto.loadTrustStore();
		} catch (CertificateException e) {
			logger.error("Unable to load Trust Store: " + e.getMessage() + "\nClient Application terminated.");
			System.exit(1);
		} catch (Exception e) {
			logger.error("Unable to load Trust Store: " + e.getMessage() + "\nServer Application terminated.");
			System.exit(1);
		}
		logger.info("---List of trusted CAs---");
		for (X509Certificate cert : trustedCAs) {
			logger.info(cert.getSubjectX500Principal().getName());
		}
	}
	
	/**
	 * Imports the client certificate from the specified path
	 * @param path Path to the client certificate file
	 * @throws CertificateException
	 * @throws FileNotFoundException
	 */
	public void importClientCertificate(String path) throws CertificateException, FileNotFoundException {
		logger.debug("Trying to import X509 client Certificate from: " + path);
		File certificateFile = new File(path);
		CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
		X509Certificate clientCert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(certificateFile));
		
		if (clientCert != null)
			session.setClientCertificate(clientCert);
		else
			logger.debug("Unable to import client certificate from " + path);
	}

	public void initLog() {
		LogSetup ls = new LogSetup("logs/client.log", name, Level.ALL);
		this.logger = ls.getLogger();
	}

	/**
	 * Try to establish Connection to the KVServer.
	 */
	@Override
	public void connect() throws UnknownHostException, IOException, InvalidMessageException, ConnectException {
		// System.out.println("New KVComm " + address + " " + port + " " + this.name);
		handshakeComplete = false;
		logger.debug("Trying to create new KVComm " + address + ":" + port);
		kvComm = new KVCommunication(address, port, this.name);
		
		try {
			performHandshake(kvComm.getSocket());
		} catch (HandshakeException e) {
			// TODO Auto-generated catch block
			logger.error("Exception during Handshake: " + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			logger.error("IO Exception during Handshake: " + e.getMessage());
			e.printStackTrace();
		} catch (SessionException ex) {
			logger.error("Error during secure handshake:\n" + ex.getMessage());
			ex.printStackTrace();
		}
		
		if (handshakeComplete) {
			KVQuery kvQueryConnectMessage = new KVQuery(KVMessage.StatusType.CONNECT);
			logger.debug("Trying to send connect message to " + address + ":" + port);
			kvComm.sendMessageEncrypted(kvQueryConnectMessage.toBytes(), session);
			logger.debug("Sent Connect message to " + address + ":" + port);
			logger.debug("Waiting for response from " + address + ":" + port);
			byte[] connectResponse = kvComm.receiveMessage(session.getEncKey(), session.getIV());
			logger.debug("Response received " + address + ":" + port);
	
			KVQuery kvQueryMessage = new KVQuery(connectResponse);
	
	
			if (kvQueryMessage.getStatus() == StatusType.CONNECT_SUCCESS) {
				if (DEBUG) {
					logger.info(moduleName + ": Connected to KVServer");
					logger.info(moduleName + ": Server Message: " + kvQueryMessage.getTextMessage());
				}
			}
	
			else if (kvQueryMessage.getStatus() == StatusType.CONNECT_ERROR) {
				if (DEBUG) {
					System.out.println(moduleName + ": Unable to connect to KVServer.");
					logger.error(moduleName + ": Unable to connect to KVServer.");
				}
			}
	
			else {
				if (DEBUG) {
					logger.error(moduleName + ": Unknown Message received from KVServer. Type: " + kvQueryMessage.getStatus().toString());
					System.out.println(moduleName + ": Unknown Message received from KVServer. Type: " + kvQueryMessage.getStatus().toString());
				}
			}
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
		kvComm.sendMessage(bytes);
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
	 * Generate random bytes for pre-master secret
	 * @param numBytes number of bytes to generate
	 * @return random bytes of specified length
	 */
	private byte[] generateMasterSecret(int numBytes) {
		byte[] p = new byte[numBytes];
		
		Random rand = new Random();
		rand.nextBytes(p);
		
		return p;
	}
	
	/**
	 * Perform the miniSSL handshake
	 * @param clientSocket socket used for communication
	 * @throws HandshakeException in case of an error
	 * @throws IOException 
	 * @throws SocketTimeoutException 
	 * @throws SessionException 
	 */
	public void performHandshake(Socket clientSocket) throws HandshakeException, SocketTimeoutException, IOException, SessionException {
		/* Create new session */
		session = new SessionInfo(this.name, Settings.TRANSFER_ENCRYPTION);
		
		session.setClientIP(clientSocket.getLocalAddress().getHostAddress());
		session.setServerIP(clientSocket.getInetAddress().getHostAddress());
		session.setLocalPort(clientSocket.getLocalPort());
		session.setRemotePort(clientSocket.getPort());
		
		/* Try to import Client Certificate */
		try {
			importClientCertificate(Settings.CLIENT_CERT_PATH);
		} catch (CertificateException e) {
			throw new IllegalArgumentException("Unable to create X.509 Certificate from file: " + Settings.CLIENT_CERT_PATH);
		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException("X.509 Certificate file not found at: " + Settings.CLIENT_CERT_PATH);
		}
		
		/* Try to import CA Certificate */
		try {
			session.setCACertificate(CommonCrypto.importCACertificate(Settings.getCACertPath()));
			if (!CommonCrypto.isCATrusted(session.getCACertificate(), this.trustedCAs)) {
				logger.error("CA Certificate: " + session.getCACertificate().getSubjectX500Principal() + " is not a trusted CA.\nClient Application terminated.");				
				System.exit(1);
			}
		} catch (CertificateException e) {
			logger.error("Unable to import CA Certificate from file: " + Settings.getCACertPath() + ", or Certificate invalid.\nReason: " + e.getMessage() + "\nClient Application Exiting.");
			System.exit(1);
		} catch (InvalidKeyException e) {
			logger.error("Unable to import CA Certificate: " + e.getMessage() + ".\nClient Application terminated.");
			System.exit(1);
		} catch (NoSuchAlgorithmException e) {
			logger.error("Unable to import CA Certificate: " + e.getMessage() + ".\nClient Application terminated.");
			System.exit(1);
		} catch (NoSuchProviderException e) {
			logger.error("Unable to import CA Certificate: " + e.getMessage() + ".\nClient Application terminated.");
			System.exit(1);
		} catch (SignatureException e) {
			logger.error("Unable to import CA Certificate: " + e.getMessage() + ".\nClient Application terminated.");
			System.exit(1);
		}
		
		
		
		// Send ClientInit
		ClientInitMessage clientInitMessage = new ClientInitMessage();
		session.setClientNonce(clientInitMessage.getNonce());
		try {
			sendObject(clientInitMessage);
		} catch (IOException e) {
			throw new HandshakeException("I/O failed while sending clientInit. Message: " + e.getMessage());
		}
		
		
		// Wait for ServerInit
		Message message = bytesToMessage(kvComm.receiveMessage(session.getEncKey(), session.getIV()));
		if (!verifyMessageType(message, MessageType.ServerInitMessage))
			throw new HandshakeException("Invalid Message received.");
		
		ServerInitMessage serverInitMessage = (ServerInitMessage) message;
		session.setServerNonce(serverInitMessage.getNonce());
		session.setClientAuthRequired(serverInitMessage.isClientAuthRequired());
		session.setServerCertificate(serverInitMessage.getCertificate());
		
		
		// Verify validity of certificate 
		try {
			CommonCrypto.verifyCertificate(session.getServerCertificate(), session.getCACertificate());
		} catch (CertificateException e) {
			//logger.error("Unable to verify the received X.509 Server Certificate: " + e.getMessage());
			throw new HandshakeException("Unable to verify the received X.509 Server Certificate: " + e.getMessage());
		} catch (InvalidKeyException e) {
			//logger.error("Invalid key for received X.509 Server Certificate: " + e.getMessage());
			throw new HandshakeException("Invalid key for received X.509 Server Certificate: " + e.getMessage());
		} catch (NoSuchAlgorithmException e) {
			//logger.error("Invalid cipher for received X.509 Server Certificate: " + e.getMessage());
			throw new HandshakeException("Invalid cipher for received X.509 Server Certificate: " + e.getMessage());
		} catch (NoSuchProviderException e) {
			//logger.error("Invalid provider for received X.509 Server Certificate file: " + e.getMessage());
			throw new HandshakeException("Invalid provider for received X.509 Server Certificate file: " + e.getMessage());
		} catch (SignatureException e) {
			//logger.error("Invalid signature for received X.509 Server Certificate file: " + e.getMessage());
			throw new HandshakeException("Invalid signature for received X.509 Server Certificate file: " + e.getMessage());
		}
		
		// Generate 47 byte random master secret
		session.setMasterSecret(generateMasterSecret(47));
		
		
		// Generate two session keys, one for encryption, one for mac
		try {
			session.setEncKey(CommonCrypto.generateSessionKey(Settings.ALGORITHM_HASHING, session.getMasterSecret(), session.getClientNonce(), session.getServerNonce(), new String("00000000").getBytes(Settings.CHARSET)));
			session.setMacKey(CommonCrypto.generateSessionKey(Settings.ALGORITHM_HASHING, session.getMasterSecret(), session.getClientNonce(), session.getServerNonce(), new String("11111111").getBytes(Settings.CHARSET)));
		} catch (IOException e) {
			throw new HandshakeException ("I/O failed during generation of session keys. Message: " + e.getMessage());
		} catch (InvalidKeyException e) {
			throw new HandshakeException ("Unable to generate Session Keys. The key was invalid. Message: " + e.getMessage());
		} catch (NoSuchAlgorithmException e) {
			throw new HandshakeException ("Unable to generate Session Keys. The specified algorithm was invalid. Message: " + e.getMessage());
		}
		
		// Generate Session hash
		try {
			session.setSecureSessionHash(CommonCrypto.generateSessionHash(Settings.ALGORITHM_HASHING, session.getMacKey(), session.getClientNonce(), session.getServerNonce(), session.getServerCertificate().getEncoded(), session.isClientAuthRequired()));
		} catch (IOException e) {
			throw new HandshakeException ("I/O failed during generation of session hash. Message: " + e.getMessage());
		} catch (InvalidKeyException e) {
			throw new HandshakeException ("Unable to generate Session Hash. The key was invalid. Message: " + e.getMessage());
		} catch (NoSuchAlgorithmException e) {
			throw new HandshakeException ("Unable to generate Session Hash. The specified algorithm was invalid. Message: " + e.getMessage());
		} catch (CertificateEncodingException e) {
			throw new HandshakeException ("Unable to generate Session Hash. The specified Certificate has invalid encoding. Message: " + e.getMessage());
		}
		
		
		// Encrypt master secret with public key from verified Server certificate
		try {
			logger.debug("THE MASTER SECRET (p): " + new String(session.getMasterSecret(), Settings.CHARSET));
		} catch (UnsupportedEncodingException e) {
			logger.debug("THE MASTER SECRET (p): <INVALID ENCODING: " + Settings.CHARSET + ">");
		}

		try {
			session.setEncryptedSecret(CommonCrypto.encrypt(session.getMasterSecret(), Settings.ALGORITHM_ENCRYPTION, session.getServerCertificate().getPublicKey()));
		} catch (InvalidKeyException e) {
			try {
				throw new HandshakeException ("Unable to encrypt master secret. The key was invalid. Message: " + e.getMessage() + "\nKey: <" + new String(session.getServerCertificate().getPublicKey().getEncoded(), Settings.CHARSET) + ">");
			} catch (UnsupportedEncodingException e1) {
				throw new HandshakeException ("Unable to encrypt master secret. The key was invalid. Message: " + e.getMessage() + "\nKey: <INVALID ENCODING: " + Settings.CHARSET + ">");
			}
		} catch (NoSuchAlgorithmException e) {
			throw new HandshakeException ("Unable to encrypt master secret. Invalid Algorithm: " + Settings.ALGORITHM_ENCRYPTION + ". Message: " + e.getMessage());
		} catch (NoSuchPaddingException e) {
			throw new HandshakeException ("Unable to encrypt master secret. No such Padding for " + Settings.ALGORITHM_ENCRYPTION + ". Message: " + e.getMessage());
		} catch (IllegalBlockSizeException e) {
			throw new HandshakeException ("Unable to encrypt master secret. Illegal Block Size for Algorithm: " + Settings.ALGORITHM_ENCRYPTION + ". Message: " + e.getMessage());
		} catch (BadPaddingException e) {
			throw new HandshakeException ("Unable to encrypt master secret. Invalid padding for: " + Settings.ALGORITHM_ENCRYPTION + ". Message: " + e.getMessage());
		}
		
		// Send ClientKeyExchangeMessage to Server
		if (session.isClientAuthRequired()) {
			// mutual authentication
			try {
				byte[] sigContent = CommonCrypto.concatenateByteArray(session.getServerNonce(), session.getEncryptedSecret());
				byte[] sigContentHash = CommonCrypto.generateHash(Settings.ALGORITHM_HASHING, session.getMacKey(), sigContent);
				byte[] signature = CommonCrypto.sign(sigContentHash, Settings.ALGORITHM_ENCRYPTION, this.clientPrivateKey);
				
				ClientKeyExchangeMessage clientKeyExchangeMessage = new ClientKeyExchangeMessage(session.getEncryptedSecret(), session.getSecureSessionHash(), session.getClientCertificate(), signature);
				
				try {
					sendObject(clientKeyExchangeMessage);
				} catch (IOException e) {
					throw new HandshakeException("I/O failed while sending clientKeyExchangeMessage. Message: " + e.getMessage());
				}
			} catch (InvalidKeyException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (NoSuchAlgorithmException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (UnsupportedEncodingException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			

		} else {
			// simple authentication
			ClientKeyExchangeMessage clientKeyExchangeMessage = new ClientKeyExchangeMessage(session.getEncryptedSecret(), session.getSecureSessionHash());
			try {
				sendObject(clientKeyExchangeMessage);
			} catch (IOException e) {
				throw new HandshakeException("I/O failed while sending clientKeyExchangeMessage. Message: " + e.getMessage());
			}
		}
		
		// Compute confirmation Hash
		try {
		if (session.isClientAuthRequired())
			session.setSecureConfirmationHash(CommonCrypto.generateConfirmationHash(Settings.ALGORITHM_HASHING, session.getMacKey(), session.getEncryptedSecret(), session.getSecureSessionHash(), session.getClientCertificate()));
		else
			session.setSecureConfirmationHash(CommonCrypto.generateConfirmationHash(Settings.ALGORITHM_HASHING, session.getMacKey(), session.getEncryptedSecret(), session.getSecureSessionHash()));
		} catch (IOException e) {
			throw new HandshakeException ("I/O failed during generation of Confirmation hash. Message: " + e.getMessage());
		} catch (InvalidKeyException e) {
			throw new HandshakeException ("Unable to generate Confirmation Hash. The key was invalid. Message: " + e.getMessage());
		} catch (NoSuchAlgorithmException e) {
			throw new HandshakeException ("Unable to generate Confirmation Hash. The specified algorithm was invalid. Message: " + e.getMessage());
		}
		
		
		
		// Wait for ServerAuthConfirmation
		// Wait for ServerInit
		message = bytesToMessage(kvComm.receiveMessage(session.getEncKey(), session.getIV()));
		if (!verifyMessageType(message, MessageType.ServerAuthConfirmationMessage))
			throw new HandshakeException("Invalid Message received.");
		
		ServerAuthConfirmationMessage serverAuthConfirmationMessage = (ServerAuthConfirmationMessage) message;
		
		// Compare generated confirmation Hash with confirmation Hash received from Server
		if (!CommonCrypto.isByteArrayEqual(session.getSecureConfirmationHash(), serverAuthConfirmationMessage.getConfirmationHash())) {
			throw new HandshakeException("Confirmation Information Mismatch. Confirmation Hash received from Server did not match Confirmation Hash computed by Client.");
		} else {
			logger.info("Successfully compared Auth Information. Confirmation Hash for Client matches Hash received from Server.");
		}
		
		session.setIV(serverAuthConfirmationMessage.getIV());
		
		logger.debug(session);
		
		// Validate all session information
		try {
			session.validateSession();
		} catch (SessionException e) {
			throw new HandshakeException("The Session failed to validate. Inconsistent Session Data. Message: " + e.getMessage());
		}
		
		logger.info("Session validated. Handshake is complete.");
		handshakeComplete = true;
	}

	/**
	 * Disconnect from the KVServer.
	 */
	@Override
	public void disconnect() {
		if (kvComm != null && kvComm.getSocketStatus() == SocketStatus.CONNECTED) {
			try {
				kvComm.sendMessageEncrypted(new KVQuery(StatusType.DISCONNECT).toBytes(), session);
			} catch (IOException ex) {
				logger.error(moduleName + ": Unable to send disconnect message, an IO Error occured:\n" + ex.getMessage());
			} catch (InvalidMessageException ex) {
				logger.error(moduleName + ": Unable to generate disconnect message, the message type was invalid.");
			}

			// TODO: WAIT FOR DISCONNECT_SUCCESS MESSAGE THEN DISCONNECT
			// As this is never sent by the server I simply close the connection for now.
			// Wait for answer
			if (DEBUG)
				logger.info(moduleName + ": Waiting for disconnect response from server...");

			try {
				byte[] disconnectResponse = kvComm.receiveMessage(session.getEncKey(), session.getIV());
				KVQuery kvQueryMessage = new KVQuery(disconnectResponse);
				if (kvQueryMessage.getStatus() == StatusType.DISCONNECT_SUCCESS) {
					if (DEBUG)
						logger.info(moduleName + ": Successfully disconnected from server.");
				} else {
					if (DEBUG)
						logger.error(moduleName + ": No or invalid response from Server, the connection will be closed forcefully.");
				}
			} catch (InvalidMessageException ex) {
				if (DEBUG)
					logger.error(moduleName + ": Unable to generate KVQueryMessage from Server response:\n" + ex.getMessage());
				// ex.printStackTrace();
			} catch (SocketTimeoutException ex) {
				if (NUM_RETRIES > currentRetries) {
					currentRetries++;
					logger.error(moduleName + ": The connection to the KVServer timed out. " +
							"Retrying (" + currentRetries + "/" + NUM_RETRIES + ")");
					this.disconnect();
				} else {
					logger.error(moduleName + ": The connection to the KVServer timed out. Closing the connection forcefully.");
				}
			} catch (IOException ex) {
				if (DEBUG) {
					logger.error(moduleName + ": Connection closed forcefully " +
							"because an IO Exception occured while waiting for PUT response from the server:\n" + ex.getMessage());
				}
			}

			if (kvComm.getSocketStatus() == SocketStatus.CONNECTED) {
				kvComm.closeConnection();
			}

		} else {
			logger.error(moduleName + ": Not connected to a KVServer (disconnect).");
		}
	}

	/**
	 * Find and connect to the responsible server coordinator for a given key according to the current meta data
	 * @param key The key that we want to find the responsible server for
	 * @return ServerData for the responsible Server
	 */
	private ServerData getResponsibleServerCoordinator(String key) {
		/* Obtain responsible server according to current meta data */
		ServerData responsibleServer = null;
		try {
			responsibleServer = consHash.getServerForKey(key);
		} catch (IllegalArgumentException ex) {
			logger.error(moduleName + ": Failed to obtain responsible server for key " + key + ": The obtained value for the server hash was of invalid format.");
			// ex.printStackTrace();
		} catch (EmptyServerDataException ex) {
			logger.error(moduleName + ": Failed to obtain responsible server for key " + key + ": There are no servers hashed to the circle.");
			// ex.printStackTrace();
		}

		return responsibleServer;
	}

	private void connectServer(String key, ServerData responsibleServer) {
		if (DEBUG)
			logger.info(moduleName + ": The responsible Server for key " + key + " is: " + responsibleServer.getAddress() + ":" + responsibleServer.getPort());
		/* Make sure we select the correct server and connect to it */
		if (!(responsibleServer.getAddress().equals(this.address)) || responsibleServer.getPort() != this.port) {
			if (DEBUG)
				logger.info(moduleName + ": We are currently not connected to the responsible server (Connected to: " + this.address + ":" + this.port);

			if (kvComm.getSocketStatus() == SocketStatus.CONNECTED) {
				if (DEBUG)
					logger.info(moduleName + ": Disconnecting from " + address + ":" + port + " (currently connected Server)");

				this.disconnect();
			}

			this.address = responsibleServer.getAddress();
			this.port = responsibleServer.getPort();

			try {
				if (DEBUG)
					logger.info(moduleName + ": Connecting to Responsible Server: " + address + ":" + port);

				this.connect();
			} catch (UnknownHostException ex) {
				logger.warn(moduleName + ": Put Request Failed. Responsible Server is Unknown Host!");
			} catch (IOException ex) {
				logger.warn(moduleName + ": Put Request Failed. Could not establish connection due to an IOError!" + ex.getMessage());
			} catch (InvalidMessageException ex) {
				logger.warn(moduleName + ": Put Request Failed. Unable to connect to responsible server. Received an invalid message: \n" + ex.getMessage());
			}
		}
	}

	/**
	 * Find a responsible server for a given key according to the current meta data
	 * @param key The key that we want to find the responsible server for
	 * @return ServerData for the responsible Server
	 */
	private ServerData getResponsibleServer(String key) {
		/* Obtain responsible server according to current meta data */
		ServerData responsibleServer = null;
		try {
			List<ServerData> servers = consHash.getServersForKey(key);
			responsibleServer = servers.get(generator.nextInt(servers.size()));
		} catch (IllegalArgumentException ex) {
			logger.error(moduleName + ": Failed to obtain responsible server for key " + key + ": The obtained value for the server hash was of invalid format.");
			// ex.printStackTrace();
		} catch (EmptyServerDataException ex) {
			logger.error(moduleName + ": Failed to obtain responsible server for key " + key + ": There are no servers hashed to the circle.");
			// ex.printStackTrace();
		}

		return responsibleServer;
	}

	/**
	 * Put a key value pair to the remote KVServer
	 * @param key The key that should be inserted
	 * @param value The value associated with the key
	 * @return KVMessage Information retrieved from the server (e.g. if operation successful)
	 */
	@Override
	public KVMessage put(String key, String value) throws ConnectException {
		/* Find & if necessary, connect to responsible Server */
		ServerData responsibleServer = getResponsibleServerCoordinator(key);
		connectServer(key, responsibleServer);

		if (kvComm.getSocketStatus() == SocketStatus.CONNECTED) {
			if (DEBUG)
				logger.info(moduleName + ": Connected to the responsible Server: " + address + ":" + port);

			try {
				/* Optimistic Query, send put request to current connected server */
				kvComm.sendMessageEncrypted(new KVQuery(StatusType.PUT, key, value).toBytes(), session);

				if (DEBUG)
					logger.info(moduleName + ": Sent PUT Request for <key, value>: <" + key + ", " + value + ">");
			} catch (IOException ex) {
				logger.error(moduleName + ": Unable to send put command, an IO Error occured during transmission:\n" + ex.getMessage());
			} catch (InvalidMessageException ex) {
				logger.error(moduleName + ": Unable to generate put command, the message type is invalid for the given arguments.");
			}

			// Wait for answer
			if (DEBUG)
				logger.info("Waiting for PUT response from server...");

			try {
				byte[] putResponse = kvComm.receiveMessage(session.getEncKey(), session.getIV());
				KVQuery kvQueryMessage = new KVQuery(putResponse);
				KVResult kvResult = new KVResult(kvQueryMessage.getStatus(), kvQueryMessage.getKey(), kvQueryMessage.getValue());
				//System.out.println(kvResult.getStatus());
				if (kvResult.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
					/* Need to update meta data and contact other server */
					if (kvResult.key.equals("metaData")) {
						/* Update stale local meta data with actual meta data from server */
						if (DEBUG)
							logger.info(moduleName + ": Received new MetaData from Server: " + kvResult.value);
						this.metaData.update(kvResult.value);

						/* Update consistent hashing circle to new version */
						this.consHash.update(metaData.getServers());
						/* Retrieve & connect responsible Server for put key */
						responsibleServer = getResponsibleServerCoordinator(key);
						connectServer(key, responsibleServer);
						if (responsibleServer == null) {
							logger.error(moduleName + ": Put Request Failed. Unable to find responsible server for key: " + key + "\nList of servers in circle: \n");
							for (String server : consHash.getHashCircle().values()) {
								logger.error(server);
							}
							return null;
						}
						/* Retry PUT */
						return this.put(key, value);
					}
				}

				// System.out.println(kvResult.getStatus());
				// PUT_SUCCESS or PUT_UPDATE
				return kvResult;
			} catch (InvalidMessageException ex) {
				logger.error(moduleName + ": Unable to generate KVQueryMessage from Server response:\n" + ex.getMessage());
				// ex.printStackTrace();
			} catch (SocketTimeoutException ex) {
				logger.error(moduleName + ": The server did not respond to the PUT Request :(. Please try again at a later time.");
			} catch (IOException ex) {
				logger.error(moduleName + ": An IO Exception occured while waiting for PUT response from the server:\n" + ex.getMessage());
				// ex.printStackTrace();
			} catch (IllegalArgumentException ex) {
				logger.error(moduleName + ": Failed to obtain responsible server for key " + key + ": The obtained value for the server hash was of invalid format.");
				// ex.printStackTrace();
			}
			return null;
		} else {
			try {
				// try to reconnect
				connect();
				return this.put(key, value); 
			} catch (UnknownHostException e) {
				throw new ConnectException(moduleName + ": Not connected to a KVServer (put). UnknownHost " + address + ":" + port);
			} catch (IOException e) {
				throw new ConnectException(moduleName + ": Not connected to a KVServer (put). IO Failure " + address + ":" + port + ", message: " + e.getMessage());
			} catch (InvalidMessageException e) {
				throw new ConnectException(moduleName + ": Not connected to a KVServer (put). Invalid Message " + address + ":" + port);
			}
		}
	}

	/**
	 * Obtain the value of a given key from the remote KVServer.
	 * @param key the key that the value should be obtained for
	 * @return KVMessage Information from the server (e.g. GET_SUCESS and value for key if successful, error otherwise).
	 */
	@Override
	public KVMessage get(String key) throws ConnectException {
		logger.warn("Trying to get Key <" + key + ">");

		/* Find & if necessary, connect to responsible Server */
		ServerData responsibleServer = getResponsibleServer(key);

		if (kvComm != null && kvComm.getSocketStatus() == SocketStatus.CONNECTED) {
			/* Optimistic query to currently connected Server */
			try {
				kvComm.sendMessageEncrypted(new KVQuery(StatusType.GET, key).toBytes(), session);

				if (DEBUG)
					logger.info(moduleName + ": Sent GET Request for <key>: <" + key + ">");
			} catch (SocketTimeoutException ex) {
				logger.error(moduleName + ": Unable to transmit GET Request :(. The connection timed out. Please try again at a later time.");
			} catch (IOException ex) {
				logger.error(moduleName + ": Unable to send get command, an IO Error occured during transmission:\n" + ex.getMessage());
			} catch (InvalidMessageException ex) {
				logger.error(moduleName + ": Unable to generate get command, the message type is invalid for the given arguments.");
			}

			// Wait for answer
			if (DEBUG)
				logger.info("Waiting for GET response from server...");

			try {
				byte[] getResponse = kvComm.receiveMessage(session.getEncKey(), session.getIV());
				KVQuery kvQueryMessage = new KVQuery(getResponse);
				KVResult kvResult = new KVResult(kvQueryMessage.getStatus(), kvQueryMessage.getKey(),kvQueryMessage.getValue());

				if (kvResult.getStatus() == StatusType.GET_SUCCESS) {
					logger.info("Received GET_SUCCESS for key " + key);
					return kvResult;
				} else if (kvResult.getStatus() == StatusType.GET_ERROR) {
					responsibleServer = getResponsibleServerCoordinator(key);
					if (responsibleServer.getPort() == port && responsibleServer.getAddress().equals(address)) {
						return kvResult;
					} else {
						address = responsibleServer.getAddress();
						port = responsibleServer.getPort();
						name = responsibleServer.getName();
						connect();
						kvComm.sendMessageEncrypted(new KVQuery(StatusType.GET, key).toBytes(), session);
						getResponse = kvComm.receiveMessage(session.getEncKey(), session.getIV());
						kvQueryMessage = new KVQuery(getResponse);
						return new KVResult(kvQueryMessage.getStatus(), kvQueryMessage.getKey(),kvQueryMessage.getValue());
					}
				} else if (kvResult.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
					/* Need to update meta data and contact other server */
					if (kvResult.key.equals("metaData")) {
						/* Update stale local meta data with actual meta data from server */
						if (DEBUG)
							logger.info(moduleName + ": Received new MetaData from Server: " + kvResult.value);
						this.metaData.update(kvResult.value);

						/* Update consistent hashing circle to new version */
						this.consHash.update(metaData.getServers());
						/* Retrieve & connect responsible Server for put key */
						responsibleServer = getResponsibleServer(key);
						connectServer(key, responsibleServer);
						if (responsibleServer == null) {
							logger.error(moduleName + ": Get Request Failed. Unable to find responsible server for key: " + key + "\nList of servers in circle: \n");
							for (String server : consHash.getHashCircle().values()) {
								logger.error(server);
							}
							return null;
						}
						/* Retry GET */
						return this.get(key);
					} else {
						throw new InvalidMessageException(moduleName + ": Invalid Response Message received from Server:\n" +
								"  Type: " + kvResult.getStatus() + "\n" +
								"  Key: " + kvResult.getKey() + "\n" +
								"  Value: " + kvResult.getValue());
					}
				}
			} catch (InvalidMessageException ex) {
				logger.error(moduleName + ": Unable to generate KVQueryMessage from Server response:\n" + ex.getMessage());
				// ex.printStackTrace();
			} catch (SocketTimeoutException ex) {
				logger.error(moduleName + ": The server did not respond to the GET REquest :(. Please try again at a later time.");
			} catch (IOException ex) {
				logger.error(moduleName + ": An IO Exception occured while waiting for GET response from the server:\n" + ex.getMessage());
				// ex.printStackTrace();
			} catch (IllegalArgumentException ex) {
				logger.error(moduleName + ": Failed to obtain responsible server for key " + key + ": The obtained value for the server hash was of invalid format.");
				// ex.printStackTrace();
			}
			return null;
		} else {
			try {
				connectServer(key, responsibleServer);
				return this.get(key); 
			} catch (IOException e) {
				throw new ConnectException(moduleName + ": Not connected to a KVServer (get). IO Failure " + address + ":" + port  + ", message: " + e.getMessage());
			}
		}
	}

	/**
	 * Obtain the current meta data for this KVStore
	 * @return {@link InfrastructureMetadata} The meta data for this instance of KVStore
	 */
	public InfrastructureMetadata getMetadata() {
		return this.metaData;
	}

	/**
	 * Obtain the current Hash-circle for this instance of KVStore
	 * @return {@link ConsistentHashing} current Hash-circle for this instance of KVStore
	 */
	public ConsistentHashing getHashCircle() {
		return this.consHash;
	}

	/**
	 * Obtain connection status of communication module
	 * @return {@link SocketStatus} connection status of communication module
	 */
	public SocketStatus getConnectionStatus() {
		if (this.kvComm != null)
			return this.kvComm.getSocketStatus();
		else {
			logger.error(moduleName + ": Cannot obtain socket status. Communication module not initialized.");
			return SocketStatus.DISCONNECTED;
		}

	}
}
