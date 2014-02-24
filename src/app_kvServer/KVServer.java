package app_kvServer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.CommonCrypto;
import common.InfrastructureMetadata;
import common.ServerData;
import common.Settings;
import common.communicator.ServerServerCommunicator;
import consistent_hashing.ConsistentHashing;
/**
 * The KVServer program which will handle multiple client request . 
 * @author Udhayaraj Sivalingam
 */
public class KVServer extends Thread {
	private final  static boolean DEBUG = false;
	private Logger logger;
	private  boolean serveClientRequest = false;
	private  boolean isWriteLocked = false;
	private  KVData kvdata = new KVData();
	private  List<HashMap<BigInteger,String>> movedDataList = new ArrayList<HashMap<BigInteger,String>>();
	private  ServerData serverData = null;
	private  int port;
	private  ServerSocket serverSocket;
	private  boolean running;
	private  InfrastructureMetadata metaData;
	private ConsistentHashing consistentHashing;

	private ServerServerCommunicator nextServer;
	private ServerServerCommunicator nextNextServer;
	private KVData lastNodeData;
	private KVData lastLastNodeData;
	
	public static String SERVER_CERT_PATH = "";
	public static String SERVER_PRIVKEY_PATH = "";
	public static X509Certificate serverCertificate = null;
	public static X509Certificate caCertificate = null;
	private static PrivateKey serverPrivateKey;
	private ArrayList<X509Certificate> trustedCAs;



	public boolean isServeClientRequest() {
		return serveClientRequest;
	}


	public void setServeClientRequest(boolean serveClientRequest) {
		this.serveClientRequest = serveClientRequest;
	}


	public boolean isWriteLocked() {
		return isWriteLocked;
	}


	public void setWriteLocked(boolean isWriteLocked) {
		this.isWriteLocked = isWriteLocked;
	}


	public KVData getKvdata() {
		return kvdata;
	}


	public void setKvdata(KVData kvdata) {
		this.kvdata = kvdata;
	}


	public List<HashMap<BigInteger, String>> getMovedDataList() {
		return movedDataList;
	}


	public void setMovedDataList(List<HashMap<BigInteger, String>> movedDataList) {
		this.movedDataList = movedDataList;
	}



	public int getPort() {
		return port;
	}


	public void setPort(int port) {
		this.port = port;
	}


	public ServerSocket getServerSocket() {
		return serverSocket;
	}


	public ConsistentHashing getConsistentHashing() {
		return consistentHashing;
	}


	public void setConsistentHashing(ConsistentHashing consistentHashing) {
		this.consistentHashing = consistentHashing;
	}


	public void setServerSocket(ServerSocket serverSocket) {
		this.serverSocket = serverSocket;
	}


	public InfrastructureMetadata getMetaData() {
		return metaData;
	}


	public void setMetaData(InfrastructureMetadata metaData) {
		logger.info("SetMetaData(): " + metaData.toString());
		this.metaData = metaData;
		if(consistentHashing == null) {
			consistentHashing = new ConsistentHashing(metaData.getServers());
			logger.info("Created new hashing circle: " + metaData.toString());
		} else {
			consistentHashing.update(metaData.getServers());
			logger.info("Updated existed hashing circle: " + metaData.toString());
		}

	}


	public boolean isDEBUG() {
		return DEBUG;
	}


	public void setRunning(boolean running) {
		this.running = running;
	}


	/**
	 * Constructs a Storage Server object which listens to connection attempts 
	 * at the given port.
	 * 
	 * @param port a port number which the Server is listening to in order to 
	 * 		establish a socket connection to a client. The port number should 
	 * 		reside in the range of dynamic ports, i.e 49152 to 65535.
	 */
	public KVServer(int port){
		KVServer.SERVER_CERT_PATH = Settings.SERVER_CERT_PATH;
		KVServer.SERVER_PRIVKEY_PATH = Settings.SERVER_PRIVKEY_PATH;
		
		this.port = port;
		
		LogSetup ls = new LogSetup("logs/server.log", "Server", Level.ALL);
		this.logger = ls.getLogger();
		
		this.logger.info("KVServer log running @port: " + port);
		
		try {
			trustedCAs = CommonCrypto.loadTrustStore();
		} catch (CertificateException e) {
			logger.error("Unable to load Trust Store: " + e.getMessage() + "\nServer Application terminated.");
			System.exit(1);
		} catch (Exception e) {
			logger.error("Unable to load Trust Store: " + e.getMessage() + "\nServer Application terminated.");
			System.exit(1);
		}
		logger.info("---List of trusted CAs---");
		for (X509Certificate cert : trustedCAs) {
			logger.info(cert.getSubjectX500Principal().getName());
		}
		
		/* Try importing Server Certificate */
		try {
			createServerCertificate(KVServer.SERVER_CERT_PATH);
		} catch (CertificateException e) {
			throw new IllegalArgumentException("Unable to create Server X.509 Certificate from file: " + KVServer.SERVER_CERT_PATH);
		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException("Server X.509 Certificate file not found at: " + KVServer.SERVER_CERT_PATH);
		}
		
		/* Try to import CA Certificate */
		try {
			importCACertificate(Settings.getCACertPath());
		} catch (CertificateException e) {
			logger.error("Unable to import CA Certificate from file: " + Settings.getCACertPath() + ", or Certificate invalid.\nReason: " + e.getMessage() + "\nServer Exiting.");
			System.exit(1);
		}

		try {
			serverPrivateKey = CommonCrypto.loadPrivateKey(SERVER_PRIVKEY_PATH, Charset.forName(Settings.CHARSET));
		} catch (FileNotFoundException e) {
			logger.error("Servers private key file not found at: " + SERVER_PRIVKEY_PATH + "\nServer Application terminated.");
			System.exit(1);
		}catch (IOException e) {
			logger.error("Unable to read this Servers private key from file: " + SERVER_PRIVKEY_PATH +
					",\nReason: " + e.getMessage() + "\nServer Application terminated.");
			System.exit(1);
		} catch (InvalidKeySpecException e) {
			logger.error("Unable to read this Servers private key from file: " + SERVER_PRIVKEY_PATH +
					",\nReason: " + e.getMessage() + "\nPlease make sure the private key file is in ENCRYPTED PKCS8 Format.\n" +
							"To convert an unencrypted PEM key with openssl use the following command:\n" +
							"openssl pkcs8 -topk8 -nocrypt -inform PEM -outform DER -in inputKey.key.pem -out pkcs8OutputKey.key.pem\n" +
							"Server Application terminated.");
			System.exit(1);
		} catch (NoSuchAlgorithmException e) {
			logger.error("Unable to read this Servers private key from file: " + SERVER_PRIVKEY_PATH +
					",\nReason: " + e.getMessage() + "\nServer Application terminated.");
			System.exit(1);
		}
	}
	
	private void createServerCertificate(String serverCertificatePath) throws CertificateException, FileNotFoundException {
		File certificateFile = new File(serverCertificatePath);
		CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
		serverCertificate = (X509Certificate) certFactory.generateCertificate(new FileInputStream(certificateFile));
	}
	
	
	public void importCACertificate(String path) throws CertificateException {
		try {
			caCertificate = CommonCrypto.importCACertificate(path);
		} catch (CertificateException e) {
			logger.error("Unable to create X.509 CA Certificate from file: " + path + 
					"\nReason: " + e.getMessage() + 
					"\nServer Application terminated.");
			System.exit(1);
		}
		
		try {
			if (!CommonCrypto.isCATrusted(caCertificate, this.trustedCAs)) {
				logger.error("CA Certificate: " + caCertificate.getSubjectX500Principal() + " is not a trusted CA.\nServer Application terminated.");				
				System.exit(1);
			}
		} catch (InvalidKeyException e) {
			throw new CertificateException("Unable to import CA Certificate: " + e.getMessage());
		} catch (NoSuchAlgorithmException e) {
			throw new CertificateException("Unable to import CA Certificate: " + e.getMessage());
		} catch (NoSuchProviderException e) {
			throw new CertificateException("Unable to import CA Certificate: " + e.getMessage());
		} catch (SignatureException e) {
			throw new CertificateException("Unable to import CA Certificate: " + e.getMessage());
		}
	}

	/**
	 * Initializes and starts the server. 
	 * Loops until the the server should be closed.
	 */
	public void run() {
		if (!Thread.interrupted()) {
			running = initializeServer();
	
			if(serverSocket != null) {
				while(isRunning()){
					try {
						Socket client = serverSocket.accept();                
						ClientConnection connection = 
								new ClientConnection(client,this);
	
	
						// store the clients for further accessing
						String ip = client.getInetAddress().getHostAddress();
						serverData = new ServerData(ip+ ":" + port, ip, port);
						Thread.currentThread().setName("SERVER " + client.getInetAddress().getHostAddress() + ":" + client.getLocalPort());
						logger.info("Client Connected  [" + client.getInetAddress().getHostAddress() + ":" + client.getPort() + "]");
	
						new Thread(connection).start();
					} catch (IOException e) {
						logger.error("Error! " +
								"Unable to establish connection. \n", e);
					}
				}
			}
			logger.info("Server stopped.");
			try {
				if (serverSocket != null)
					serverSocket.close();
			} catch (IOException e) {
				logger.error("not able to close the server socket" + e.getMessage());
			}
		}
	}

	private boolean isRunning() {
		return this.running;
	}

	/**
	 * Stops the server insofar that it won't listen at the given port any more.
	 */

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);

			logger.info("Server listening on port: " 
					+ serverSocket.getLocalPort());   
			
			// init with local meta data
			ArrayList<ServerData> serverData = new ArrayList<ServerData>();
			String address = InetAddress.getLocalHost().getHostAddress();
			System.out.println(address);
			serverData.add(new ServerData(address + ":" + port, address, port));
			setMetaData(new InfrastructureMetadata(serverData));
			
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}


	public ServerData getServerData() {
		return serverData;
	}


	public void setServerData(ServerData serverData) {
		this.serverData = serverData;
	}


	/**
	 * @return communicator to the next server in the hash circle,
	 *  i.e. one of the servers that holds replicated data of this KVServer
	 */
	public ServerServerCommunicator getNextServer() {
		return nextServer;
	}


	/**
	 * @param changes the communicator to the next server in the hash circle,
	 *  i.e. one of the servers that holds replicated data of this KVServer
	 */
	public void setNextServer(ServerServerCommunicator nextServer) {
		this.nextServer = nextServer;
	}


	/**
	 * @return communicator to the second next server in the hash circle,
	 *  i.e. one of the servers that holds replicated data of this KVServer
	 */
	public ServerServerCommunicator getNextNextServer() {
		return nextNextServer;
	}


	/**
	 * @param changes the communicator to the second next server in the hash circle,
	 *  i.e. one of the servers that holds replicated data of this KVServer
	 */
	public void setNextNextServer(ServerServerCommunicator nextNextServer) {
		this.nextNextServer = nextNextServer;
	}


	/**
	 * @return the replicated data held from the last server in the hash circle
	 */
	public KVData getLastNodeData() {
		return lastNodeData;
	}


	/**
	 * @param change the replicated data held from the last server in the hash circle
	 */
	public void setLastNodeData(KVData lastNodeData) {
		this.lastNodeData = lastNodeData;
	}


	/**
	 * @return the replicated data held from the second last server in the hash circle
	 */
	public KVData getLastLastNodeData() {
		return lastLastNodeData;
	}


	/**
	 * @param change the replicated data held from the second last server in the hash circle
	 */
	public void setLastLastNodeData(KVData lastLastNodeData) {
		this.lastLastNodeData = lastLastNodeData;
	}


	/**
	 * Main entry point for the storage server application. 
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {
		if (!DEBUG) {
			try {
				if(args.length != 1) {
					System.out.println("Error! Invalid number of arguments!");
					System.out.println("Usage: Server <port>!");
				} else {
					int port = Integer.parseInt(args[0]);
					KVServer server = new KVServer(port);
					server.start();
					
					// server.setServeClientRequest(true);

					//System.exit(0);
				}
			} catch (NumberFormatException nfe) {
				System.out.println("Error! Invalid argument <port>! Not a number!");
				System.out.println("Usage: Server <port>!");
				System.exit(1);
			}
		} else {
			try {
				System.setProperty("file.encoding", "US-ASCII");
				int port = 50000;

				new KVServer(port).start();
			} catch (NumberFormatException nfe) {
				System.out.println("Error! Invalid argument <port>! Not a number!");
				System.out.println("Usage: Server <port>!");
				System.exit(1);
			}
		}
	}

}
