package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVServer extends Thread {
	private final static boolean DEBUG = false;
	private static Logger logger = Logger.getRootLogger();

	private int port;
	private ServerSocket serverSocket;
	private boolean running;

	/**
	 * Constructs a (Echo-) Server object which listens to connection attempts 
	 * at the given port.
	 * 
	 * @param port a port number which the Server is listening to in order to 
	 * 		establish a socket connection to a client. The port number should 
	 * 		reside in the range of dynamic ports, i.e 49152 – 65535.
	 */
	public KVServer(int port){
		this.port = port;
	}
	/* public KVServer(int port, String levelString){
		this.port = port;
		this.setLevel(levelString);
	}
	private String setLevel(String levelString) {

		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}*/
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
	public void stopServer(){
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
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

	/**
	 * Main entry point for the echo server application. 
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
/*public static void main(String[] args) {
		try {
			new LogSetup("logs/server.log", Level.ALL);
			if(args.length != 2) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <loglevel(ALL/DEBUG/INFO/ERROR/WARN/OFF>!");
			} else {
				int port = Integer.parseInt(args[0]);
				String levelString = args[1];
				if(levelString.equals(LogSetup.UNKNOWN_LEVEL)) {
					System.out.println("No valid log level!");
					System.out.println("Usage: Server <port> <loglevel(ALL/DEBUG/INFO/ERROR/WARN/OFF>!");
				}
				else
				{
					new KVServer(port,levelString).start();

				}
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
	}

}*/
