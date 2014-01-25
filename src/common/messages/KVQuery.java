package common.messages;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Create a message from or to client/server.
 * Implements the protocol established for this application
 * @author Claynon de Souza
 *
 */
public class KVQuery implements KVMessage {
	private StatusType command;
	private String key;
	private String value;
	private int numArgs;

	private String[] arguments;

	private  Logger logger;

	public void initLog() {
		LogSetup ls = new LogSetup("logs/KVQuery.log", "KVQuery", Level.ALL);
		this.logger = ls.getLogger();
	}
	/**
	 * Construct a query from a message received in the form of an array of bytes
	 * @param bytes
	 * @throws InvalidMessageException
	 */
	public KVQuery(byte[] bytes) throws InvalidMessageException {
		initLog();
		String message;

		message = new String(bytes);
		// removing /r of the end if necessary
		if (message.endsWith("\r")) {
			message = message.substring(0, message.length() - 1);
		}
		arguments = message.split("\n");

		if (arguments.length >= 1 && arguments.length <= 3 && bytes.length <= DROP_SIZE) {
			setType(arguments[0]);

			if (arguments.length == 1) {
				key = "";
				value = "";
			} else if (arguments.length == 2) {
				key = arguments[1];
				value = "";
			} else if (arguments.length == 3) {
				key = arguments[1];
				value = arguments[2];
			}
		} else {
			throw new InvalidMessageException("Incorrect number of arguments or size of message exceeded.");
		}
	}

	/**
	 * Constructs an query that consists only of a command.
	 * @param command the type of the query
	 * @throws InvalidMessageException thrown when a command that is not associated with exactly one argument is entered
	 */
	public KVQuery(StatusType command) throws InvalidMessageException {
		initLog();
		
		if (command != StatusType.CONNECT && command != StatusType.CONNECT_ERROR
				&& command != StatusType.DISCONNECT && command != StatusType.DISCONNECT_SUCCESS )
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");

		this.command = command;
		this.numArgs = 1;
	}

	/**
	 * Constructs an query with only one argument.
	 * @param command the type of the query
	 * @param argument  may contain the key (key-value) of the query or the message from a connection. Depends on the command. 
	 * @throws InvalidMessageException thrown when a command that is not associated with exactly one argument is entered
	 */
	public KVQuery(StatusType command, String argument) throws InvalidMessageException {
		initLog();
		
		if (command != StatusType.GET && command != StatusType.GET_ERROR && command != StatusType.GET_SUCCESS
				&& command != StatusType.FAILED && command != StatusType.CONNECT_SUCCESS)
			throw new InvalidMessageException("Incorrect number of arguments for the command");

		this.command = command;
		this.key = argument;
		this.numArgs = 2;
	}

	/**
	 * Constructs an query with a key and value.
	 * @param command the type of the query
	 * @param key the key (key-value) of the query
	 * @param value the value (key-value) of the query
	 * @throws InvalidMessageException thrown when a command associated with less than two arguments is entered
	 */
	public KVQuery(StatusType command, String key, String value) throws InvalidMessageException {
		initLog();
		
		if (command != StatusType.GET_SUCCESS
				&& command != StatusType.PUT			&& command != StatusType.PUT_SUCCESS
				&& command != StatusType.PUT_UPDATE		&& command != StatusType.PUT_ERROR
				&& command != StatusType.DELETE_SUCCESS && command != StatusType.DELETE_ERROR
				&& command != StatusType.SERVER_STOPPED && command != StatusType.SERVER_WRITE_LOCK 
				&& command != StatusType.SERVER_NOT_RESPONSIBLE) {
			throw new InvalidMessageException("Incorrect number of arguments for the command.");
		}
		this.command = command;
		this.key = key;
		this.value = value;
		this.numArgs = 3;
	}

	/**
	 * Transform the query to an array of bytes to be sent to a client or server.
	 * Marshalling method.
	 * 
	 * @return an array of bytes with the query ready to be sent. Returns null if an error occurs with the buffer.
	 */
	public byte[] toBytes() {
		byte[] bytes;
		if (numArgs == 1) {
			String message = command.toString() + "\r";
			bytes = message.getBytes();
		} else if (numArgs == 2) {
			String message = command.toString() + "\n" + key + "\r";
			bytes = message.getBytes();
		} else if (numArgs == 3) {
			String message = command.toString() + "\n" + key + "\n" + value + "\r";
			bytes = message.getBytes();
		} else {
			logger.error("Cannot convert KVQuery to bytes, since it has an incorrect number of arguments. (" + numArgs + ")");
			return null;
		}

		if (bytes.length > DROP_SIZE) {
			logger.error("Cannot convert KVQuery to bytes, since the payload would be too large.\n"
					+ "  Payload: " + bytes.length / 1024 + " kb"
					+ "  Maxmium allowed: " + DROP_SIZE / 1024 + " kb");
			return null;
		}

		return bytes;
	}

	/**
	 * Get the type of command the message is
	 * @return the command of the message
	 */
	public StatusType getStatus() {
		return this.command;
	}

	/**
	 * Get the key of a key-value query
	 * @return the key of a key-value query
	 * @throws InvalidMessageException if the query does not has a key
	 */
	public String getKey() throws InvalidMessageException {
		return this.key;
	}

	/**
	 * Get a text message from connection established or failed message sent from server
	 * @return text message from connection established or failed message
	 * @throws InvalidMessageException if the query is not of the types CONNECT or FAILED 
	 */
	public String getTextMessage() throws InvalidMessageException {
		if (!(command.equals(StatusType.CONNECT_SUCCESS) || command.equals(StatusType.FAILED))) {
			throw new InvalidMessageException("This command doesn't have a text message. " + command.toString());
		}
		return this.key;
	}

	/**
	 * Get the value of a key-value query
	 * @return the value of a key-value query
	 * @throws InvalidMessageException if the message does not has a value
	 */
	public String getValue() throws InvalidMessageException {
		if (this.value == null) {
			throw new InvalidMessageException("This command doesn't have a value. " + command.toString());
		}
		return this.value;
	}

	private void setType(String command) throws InvalidMessageException {
		try {
			this.command = StatusType.valueOf(command);
		} catch (Exception ex) {
			throw new InvalidMessageException("This code does not represent a command.");        
		}
	}
}