package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.UnknownHostException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.InvalidMessageException;
import common.messages.KVMessage;

/**
 * Simple command line Shell based on provided skeleton-code used for user-interaction with KVClient.
 * @author Elias Tatros
 */
public class Shell {
	private Logger logger;
	private static final String PROMPT = "KVClient> ";
	private BufferedReader stdin;
	private KVClient kvClient;
	private boolean stop = false;
	
	private String serverAddress;
	private int serverPort;
	
	/**
	 * Constructor for the Shell
	 * @param kvClient a KVClient instance
	 */
	public Shell(KVClient kvClient)
	{
		this.kvClient = kvClient;
		this.logger = kvClient.getLogger();
	}
	
	/**
	 * Shell main loop, calls handleCommand on user-input
	 */
	public void display() {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}
		
	/**
	 * Handles a certain set of commands, calling the respective KVClient methods and displaying return results.
	 * @param cmdLine command entered by the user
	 */
	private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");

		if(tokens[0].equals("quit")) {	
			stop = true;
			try {
			kvClient.disconnect();
			System.out.println(PROMPT + "Application exit!");
			} catch (ConnectException ex) {
				logger.error(kvClient.getClientName() + ": Connection Error: " + ex.getMessage());
			}
		
		} else if (tokens[0].equals("connect")){
			if(tokens.length == 3) {
				try{
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					if (serverPort > 0 && serverPort <= 65535) {
						kvClient.connect(serverAddress, serverPort);
					}
					else {
						System.out.println(kvClient.getClientName() + ": Invalid Port.");
						logger.error(kvClient.getClientName() + ": Invalid Port Number");
					}
				} catch(NumberFormatException nfe) {
					// printError("No valid address. Port must be a number!");
					logger.warn(kvClient.getClientName() + ": Unable to parse argument <port>");
				} catch (UnknownHostException e) {
					// printError("Unknown Host!");
					logger.warn(kvClient.getClientName() + ": Unknown Host!");
				} catch (ConnectException ex) {
					logger.warn(kvClient.getClientName() + ": Could not establish connection! Reason: " + ex.getMessage());
				} catch (IOException ex) {
					// printError("Could not establish connection!");
					logger.warn(kvClient.getClientName() + ": Could not establish connection! Reason: " + ex.getMessage());
				} catch (InvalidMessageException ex) {
					System.out.println(kvClient.getClientName() + ": Unable to connect to server. Received an invalid message: \n" + ex.getMessage());
					// ex.printStackTrace();
				}
			} else {
				printError(kvClient.getClientName() + ": Invalid number of parameters!");
			}
			
		} else  if (tokens[0].equals("put")) {
			if(tokens.length >= 3) {
				String key = tokens[1];
				StringBuilder value = new StringBuilder();
				for(int i = 2; i < tokens.length; i++) {
					value.append(tokens[i]);
					if (i != tokens.length -1 ) {
						value.append(" ");
					}
				}	
				
				try {
					System.out.println(kvClient.getClientName() + ": \n>>>> Sending PUT request for <" + key + ", " + value + ">\n");
					KVMessage kvResult = kvClient.put(key, value.toString());
					try {
						System.out.println(kvClient.getClientName() + ": \n>>> Received: " + kvResult.getStatus().toString() + ", key: " + kvResult.getKey() + ", value: " + kvResult.getValue());
					} catch (InvalidMessageException ex) {
						logger.error(kvClient.getClientName() + ": Unable to read the return Message. Reason: " + ex.getMessage());
					} catch (NullPointerException ex) {
						logger.error(kvClient.getClientName() + ": Server did not respons to PUT Request.");
					}
				} catch (ConnectException ex) {
					logger.error(kvClient.getClientName() + ": Unable to use Put command: " + ex.getMessage());
				}
				
			} else {
					printError(kvClient.getClientName() + ": Invalid number of arguments for put operation.");
			}
			
		} else  if (tokens[0].equals("get")) {
			if(tokens.length == 2) {
				String key = tokens[1];
				
				try {
					System.out.println(kvClient.getClientName() + ": \n>>> Sending GET request for key " + key);
					KVMessage kvResult = kvClient.get(key);
					try {
						if (kvResult != null)
							System.out.println(kvClient.getClientName() + ": \n>>> Received: " + kvResult.getStatus().toString() + ", value: " + kvResult.getKey());
						else
							System.out.println(kvClient.getClientName() + ": Unexpected error: The kvResult was null");
					} catch (InvalidMessageException ex) {
						System.out.println(kvClient.getClientName() + ": Unable to read the return Message. Reason: " + ex.getMessage());
					}
				} catch (ConnectException ex) {
					logger.error(kvClient.getClientName() + ": Unable to use Put command: " + ex.getMessage());
				}
				
			} else {
					printError(kvClient.getClientName() + ": Invalid number of arguments for get operation.");
			}
			
		} else if(tokens[0].equals("disconnect")) {
			try {
				if (kvClient != null)
					kvClient.disconnect();
				else {
					System.out.println(kvClient.getClientName() + ": You must connect first.");
				}
			} catch (ConnectException ex) {
				logger.error(kvClient.getClientName() + ": Connection error: " + ex.getMessage());
			}
			
		} else if(tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			
		} else if(tokens[0].equals("help")) {
			printHelp();
		} else {
			printError(kvClient.getClientName() + ": Unknown command");
			printHelp();
		}
	}
	
	/**
	 * Print the available commands.
	 */
	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t Send put request for given key and value to the connected server. \n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t Send get request for given key to the connected server. \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}
	
	private void printPossibleLogLevels() {
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
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
	}
	
	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}
}
