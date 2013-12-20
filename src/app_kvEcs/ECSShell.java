package app_kvEcs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ECSShell {

	private BufferedReader stdin;
	private static final String PROMPT = "ECS> ";
	private boolean stop = false;
	private Logger logger;

	public ECSShell() {
		LogSetup ls = new LogSetup("logs\\ecs.log", "ECS Shell", Level.ALL);
		this.logger = ls.getLogger();
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
				handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("ECS UI does not respond - Application terminated ");
			}
		}
	}

	/**
	 * Print the available commands.
	 */
	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECS HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("initService <numberOfNodes>");
		sb.append("\t Randomly choose <numberOfNodes> servers from the available machines and "
				+ "start the KVServer by issuing a SSH call to the respective machine. "
				+ "This call launches the server. All servers are initialized with the meta­data "
				+ "and remain in state stopped.\n");
		sb.append(PROMPT).append("start");
		sb.append("\t\t\t\t Starts the service (i.e. all servers instances that participate in the service.)\n");
		sb.append(PROMPT).append("stop");
		sb.append("\t\t\t\t Stops the service (i.e. all participating KVServers are stopped for "
				+ "processing client requests but the processes remain running.\n");
		sb.append(PROMPT).append("shutDown");
		sb.append("\t\t\t\t Stops all server instances and exits the remote processes.\n");
		sb.append(PROMPT).append("addNode");
		sb.append("\t\t\t\t Add a new node to the storage service at an arbitrary position.\n");
		sb.append(PROMPT).append("removeNode");
		sb.append("\t\t\t\t Remove a node from the storage service at an arbitrary position.\n");
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t\t exits the program");
		System.out.println(sb.toString());
	}

	private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");

		if (tokens.length == 1) {
			if (tokens[0].equals("quit")) {
				stop = true;
				ECSClient.ecs.shutDown();
				System.out.println(PROMPT + "Application exit!");
			} else if (tokens[0].equals("start")) {
				ECSClient.ecs.start();
			} else if (tokens[0].equals("stop")) {
				ECSClient.ecs.stop();
			} else if (tokens[0].equals("shutDown")) {
				ECSClient.ecs.shutDown();
			} else if (tokens[0].equals("addNode")) {
				ECSClient.ecs.addNode();
			} else if (tokens[0].equals("removeNode")) {
				ECSClient.ecs.removeNode();
			} else if (tokens[0].equals("help")) {
				printHelp();
			} else {
				printError("Unknown command");
			}
		} else if (tokens.length == 2) {
			if (tokens[0].equals("initService")) {
				int numberOfNodes = Integer.parseInt(tokens[1]);
				ECSClient.ecs.initService(numberOfNodes);
			} else if (tokens[0].equals("logLevel")) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + "Log level changed to level " + level);
				}
			}
		} else {
			printError("Invalid number of parameters!");
		}
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

	private void printError(String error) {
		System.out.println(PROMPT + "Error! " +  error + "\n Type 'help' if you need any help.");
	}
}
