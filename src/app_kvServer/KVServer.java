package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVServer extends Thread {
	private final static boolean DEBUG = false;
	private static Logger logger = Logger.getRootLogger();
	public static KVData kvdata = new KVData();
	private String ip;
	private int port;
	private List<ClientConnection> clientList = new ArrayList<ClientConnection>();
	private ServerSocket serverSocket;
	private boolean running;

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
	 * Initializes and starts the server but blocked for client requests
	 */
	// convert with meta data parameter
	public void initKVServer()
	{
		try {
			new LogSetup("logs/server.log", Level.ALL);
			System.setProperty("file.encoding", "US-ASCII");
			int port = 50000;
			String ip = "localhost";
			new KVServer(port).start();

		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();

		} catch (NumberFormatException nfe) {
			logger.error("Invalid port number" + nfe.getMessage());
			
		}
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
							new ClientConnection(client);
					new Thread(connection).start();
					// store the clients for further accessing
					clientList.add(connection);
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
	public void start()
	{
		
		logger.info("Allowing server to serve client requests");
		changeServeClientStatus(true,1);
		
	}
	public void stopServer(){		
		logger.info("Stopping server to serve client requests");
		changeServeClientStatus(false,1);
		
	}
	 public void lockWrite()
	    {
		 logger.info("locking server to write requests");
		 changeServeClientStatus(true,2);
	    }
	 public void UnLockWrite()
	    {
		 logger.info("unlocking server to write requests");
		 changeServeClientStatus(false,2);
	    }
	private void changeServeClientStatus(boolean status,int flag)
	{
		ClientConnection connection;
		for (Iterator<ClientConnection> connectionIterator = this.clientList.iterator(); connectionIterator.hasNext(); )
	    {
			connection = connectionIterator.next();
		if(connection.isOpen())
			{
			if(flag == 1)
				connection.setServeClientRequest(status);
			else if(flag == 2)
				connection.setWriteLocked(status);
			}
			else
			{
				this.clientList.remove(connection);
			}
		}
	}

	public void shutDown(){
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
		System.exit(1);
	}
   public void update()
   {
	   // update the metadata
   }
   public void moveData(KVServer kvserver)
   {
	  kvdata.moveData(kvserver);
   }
	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
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

	
}
