package common.messages;

import app_kvServer.KVData;

public class ServerServerMessage {
	private ServerServerStatustype command;
	private String key;
	private String value;
	private KVData serverData;

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
			checkLength(arguments.length);
			switch (command) {
			case SERVER_PUT_ALL:
				//TODO figure out how to discover which server is sending the message
				//TODO implement this constructor
				serverData = new KVData(arguments[2]);
				break;
			case SERVER_PUT:
				key = arguments[1];
				value = arguments[2];
				break;
			case SERVER_DELETE:
				key = arguments[1];
				break;
			}
		}
	}
	
	public ServerServerMessage(ServerServerStatustype command, String key) {
		if (command != ServerServerStatustype.SERVER_DELETE) {
			//throw
		}
		this.command = command;
		this.key = key;
		this.value = null;
		serverData = null;
	}
	
	public ServerServerMessage(ServerServerStatustype command, String key, String value) {
		if (command != ServerServerStatustype.SERVER_PUT) {
			//throw
		}
		this.command = command;
		this.key = key;
		this.value = value;
		serverData = null;
	}

	public byte[] toBytes() {
		byte[] bytes = null;
		String message = command.toString() + "\n";

		switch (command) {
		case SERVER_DELETE:
			message += key + "\n\r";
			break;
		case SERVER_PUT:
			message += key + "\n" + value + "\n\r";
			break;
		case SERVER_PUT_ALL:
			break;
		}
		
		bytes = message.getBytes();
		/*if (bytes.length > KVMessage.DROP_SIZE) {
			logger.error("Cannot convert KVQuery to bytes, since the payload would be too large.\n"
					+ "  Payload: " + bytes.length / 1024 + " kb"
					+ "  Maxmium allowed: " + KVMessage.DROP_SIZE / 1024 + " kb");
			return null;
		}*/

		return bytes;
	}
	
	private void checkLength(int length) throws InvalidMessageException {
		switch (command) {
		case SERVER_PUT_ALL:
		case SERVER_PUT:
			if (length != 3) {
				throw new InvalidMessageException("Incorrect number of arguments");
			}
			break;
		case SERVER_DELETE:
			if (length != 2) {
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

	private void setType(String command) throws InvalidMessageException {
		try {
			this.command = ServerServerStatustype.valueOf(command);
		} catch (Exception ex) {
			throw new InvalidMessageException("This code does not represent a command.");        
		}
	}
}
