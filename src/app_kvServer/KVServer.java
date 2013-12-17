package app_kvServer;

import java.io.IOException;
import java.math.BigInteger;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.InfrastructureMetadata;

public class KVServer extends Thread {



	private final  static boolean DEBUG = true;
	private  Logger logger = Logger.getRootLogger();
	private  boolean serveClientRequest = false;
	private  boolean isWriteLocked = false;
	private  KVData kvdata = new KVData();
	private  List<HashMap<BigInteger,String>> movedDataList = new ArrayList<HashMap<BigInteger,String>>();
	private  String ip;
	private  int port;
	private  ServerSocket serverSocket;
	private  boolean running;
	private  InfrastructureMetadata metaData;
	
	

	public Logger getLogger() {
		return logger;
	}


	public void setLogger(Logger logger) {
		this.logger = logger;
	}


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


	public String getIp() {
		return ip;
	}


	public void setIp(String ip) {
		this.ip = ip;
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


	public void setServerSocket(ServerSocket serverSocket) {
		this.serverSocket = serverSocket;
	}


	public InfrastructureMetadata getMetaData() {
		return metaData;
	}


	public void setMetaData(InfrastructureMetadata metaData) {
		this.metaData = metaData;
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
	 * 		reside in the range of dynamic ports, i.e 49152 – 65535.
	 */
	public KVServer(int port){
		this.port = port;

	}

	
	/**
	 * Initializes and starts the server. 
	 * Loops until the the server should be closed.
	 */
	public void run() {

		running = initializeServer();

		if(serverSocket != null) {
			while(isRunning()){
				try {
					Socket client = serverSocket.accept();                
					ClientConnection connection = 
							new ClientConnection(client,this);
					new Thread(connection).start();
					// store the clients for further accessing
					
					logger.info("Connected to " 
							+ client.getInetAddress().getHostName() 
							+  " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
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
			ip = serverSocket.getInetAddress().toString();
			logger.info("Server listening on port: " 
					+ serverSocket.getLocalPort());    
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}


	/**
	 * Main entry point for the storage server application. 
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {
		if (!DEBUG) {
			try {
				new LogSetup("logs/server.log", Level.ALL);
				if(args.length != 1) {
					System.out.println("Error! Invalid number of arguments!");
					System.out.println("Usage: Server <port>!");
				} else {
					int port = Integer.parseInt(args[0]);
					new KVServer(port).start();
				}
			} catch (IOException e) {
				System.out.println("Error! Unable to initialize logger!");
				e.printStackTrace();
				System.exit(1);
			} catch (NumberFormatException nfe) {
				System.out.println("Error! Invalid argument <port>! Not a number!");
				System.out.println("Usage: Server <port>!");
				System.exit(1);
			}
		} else {
			try {
				new LogSetup("logs/server.log", Level.ALL);
				System.setProperty("file.encoding", "US-ASCII");
				int port = 50000;
				
				new KVServer(port).start();

			} catch (IOException e) {
				System.out.println("Error! Unable to initialize logger!");
				e.printStackTrace();
				System.exit(1);
			} catch (NumberFormatException nfe) {
				System.out.println("Error! Invalid argument <port>! Not a number!");
				System.out.println("Usage: Server <port>!");
				System.exit(1);
			}
		}
	}

}
