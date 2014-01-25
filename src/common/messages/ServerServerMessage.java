package common.messages;

import app_kvServer.KVData;

public class ServerServerMessage {
	private ServerServerStatustype command;
	private String key;
	private String value;
	private KVData serverData;
	private int numServer;
	private final String EMPTY = "EMPTY";

	/**
	 * Construct a message received in the form of an array of bytes.
	 * @param bytes Message received in the form of bytes.
	 * @throws InvalidMessageException Thrown when the message does not have the correct number of arguments or the command is not associated if the number of arguments given.
	 */
	public ServerServerMessage(byte[] bytes) throws InvalidMessageException {
		String message;
		String[] arguments;

		message = new String(bytes);
		// removing /r of the end if necessary
		if (message.endsWith("\r")) {
			message = message.substring(0, message.length() - 1);
		}
		arguments = message.split("\n");

		if (bytes.length <= KVMessage.DROP_SIZE) {
			setType(arguments[0]);
			numServer = Integer.parseInt(arguments[1]);
			checkLength(arguments.length);
			switch (command) {
			case SERVER_PUT_ALL:
				if (arguments[2].equals(EMPTY)) {
					serverData = new KVData();
				} else {
					serverData = new KVData(arguments[2]);
				}
				break;
			case SERVER_PUT:
				key = arguments[2];
				value = arguments[3];
				break;
			case SERVER_DELETE:
				key = arguments[2];
				break;
			}
		}
	}

	/**
	 * Constructs a message that has only a key
	 * @param command The type of the message.
	 * @param numServer Indicates the server to which the message is going, according to its position in the hash circle in relation to the sender.
	 * @param key the key of the tuple
	 */
	public ServerServerMessage(ServerServerStatustype command, int numServer, String key) throws InvalidMessageException {
		if (command != ServerServerStatustype.SERVER_DELETE) {
			throw new InvalidMessageException("Incorrect number of arguments or command.");
		}
		this.command = command;
		this.numServer = numServer;
		this.key = key;
		value = null;
		serverData = null;
		numServer = 0;
	}

	/**
	 * Constructs a message that has a key and a value
	 * @param command The type of the message.
	 * @param numServer Indicates the server to which the message is going, according to its position in the hash circle in relation to the sender.
	 * @param key the key of the tuple
	 * @param value the value of the tuple
	 */
	public ServerServerMessage(ServerServerStatustype command, int numServer, String key, String value) throws InvalidMessageException {
		if (command != ServerServerStatustype.SERVER_PUT) {
			throw new InvalidMessageException("Incorrect number of arguments or command.");
		}
		this.command = command;
		this.numServer = numServer;
		this.key = key;
		this.value = value;
		serverData = null;
		numServer = 0;
	}

	/**
	 * Constructs a message to update the replicated data on other servers
	 * @param command The type of the message.
	 * @param numServer Indicates the server to which the message is going, according to its position in the hash circle in relation to the sender.
	 */
	public ServerServerMessage(ServerServerStatustype command, int numServer, KVData serverData) throws InvalidMessageException {
		if (command != ServerServerStatustype.SERVER_PUT_ALL) {
			throw new InvalidMessageException("Incorrect number of arguments or command.");
		}
		this.command = command;
		this.numServer = numServer;
		this.serverData = serverData;
		key = null;
		value = null;
	}

	/**
	 * Transform the message in an array of bytes to be sent.
	 * @return The message in an array of bytes.
	 */
	public byte[] toBytes() {
		String message = command.toString() + "\n" + numServer + "\n";

		switch (command) {
		case SERVER_DELETE:
			message += key + "\r";
			break;
		case SERVER_PUT:
			message += key + "\n" + value + "\r";
			break;
		case SERVER_PUT_ALL:
			if (serverData.toString().length() != 0) {
				message += serverData.toString() + "\r";
			} else {
				message += EMPTY + "\r";
			}
			break;
		}

		return message.getBytes();
	}

	private void checkLength(int length) throws InvalidMessageException {
		switch (command) {
		case SERVER_PUT:
			if (length != 4) {
				throw new InvalidMessageException("Incorrect number of arguments");
			}
			break;
		case SERVER_PUT_ALL:
		case SERVER_DELETE:
			if (length != 3) {
				throw new InvalidMessageException("Incorrect number of arguments");
			}
			break;
		}
	}


	public ServerServerStatustype getCommand() {
		return command;
	}

	public String getKey() {
		return key;
	}

	public String getValue() {
		return value;
	}

	public int getNumServer() {
		return numServer;
	}

	private void setType(String command) throws InvalidMessageException {
		try {
			this.command = ServerServerStatustype.valueOf(command);
		} catch (Exception ex) {
			throw new InvalidMessageException("This code does not represent a command.");        
		}
	}

	public KVData getData() {
		return serverData;
	}
}
