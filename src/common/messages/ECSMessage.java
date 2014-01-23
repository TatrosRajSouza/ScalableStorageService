package common.messages;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map.Entry;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Create a message from or to ECS/Server.
 * Implements the protocol established for this application
 * @author Claynon de Souza
 *
 */
public class ECSMessage {

	private ECSStatusType command;
	private InfrastructureMetadata metadata;
	private BigInteger startIndex;
	private BigInteger endIndex;
	private ServerData server;
	private HashMap<BigInteger, String> movingData;
	private final String movingDataEmpty = "EMPTY"; 

	private Logger logger;

	/**
	 * Construct a message received in the form of an array of bytes.
	 * @param bytes Message received in the form of bytes.
	 * @throws InvalidMessageException Thrown when the message does not have the correct number of arguments or the command is not associated if the number of arguments given.
	 */
	public ECSMessage(byte[] bytes) throws InvalidMessageException {
		LogSetup ls = new LogSetup("logs\\ecs.log", "ECSMessage", Level.ALL);
		this.logger = ls.getLogger();
		
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

			switch (arguments.length) {
			case 1:
				if (command == ECSStatusType.INIT || command == ECSStatusType.UPDATE
				|| command == ECSStatusType.MOVE_DATA || command == ECSStatusType.MOVE_DATA_INTERNAL) {
					throw new InvalidMessageException("Incorrect number of arguments or command.");
				}
				metadata = null;
				startIndex = null;
				endIndex = null;
				server = null;
				break;
			case 2:
				if (command == ECSStatusType.INIT || command == ECSStatusType.UPDATE) {
					metadata = new InfrastructureMetadata(arguments[1]);
					movingData = null;
				} else if (command == ECSStatusType.MOVE_DATA_INTERNAL){
					createMovingData(arguments[1]);
					metadata = null;
				} else {
					throw new InvalidMessageException("Incorrect number of arguments or command.");
				}
				startIndex = null;
				endIndex = null;
				server = null;
				break;
			case 6:
				if (command != ECSStatusType.MOVE_DATA) {
					throw new InvalidMessageException("Incorrect number of arguments or command.");
				}
				metadata = null;
				startIndex = new BigInteger(arguments[1]);
				endIndex = new BigInteger(arguments[2]);
				server = new ServerData(arguments[3], arguments[4], Integer.parseInt(arguments[5]));
				break;
			default:
				throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
			}
		} else {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
	}

	/**
	 * Constructs a message that consists only of a command.
	 * @param command The type of the message.
	 * @throws InvalidMessageException Thrown when a command that requires arguments is entered.
	 */
	public ECSMessage(ECSStatusType command) throws InvalidMessageException {
		LogSetup ls = new LogSetup("logs\\ecs.log", "ECSMessage", Level.ALL);
		this.logger = ls.getLogger();
		
		if (command == ECSStatusType.INIT || command == ECSStatusType.UPDATE
				|| command == ECSStatusType.MOVE_DATA || command == ECSStatusType.MOVE_DATA_INTERNAL) {
			throw new InvalidMessageException("Incorrect number of arguments or command.");
		}
		this.command = command;
	}

	/**
	 * Construct a message with metadata.
	 * @param command The type of the message.
	 * @param metadata The metadata that will be sent to the server. 
	 * @throws InvalidMessageException Thrown when the command is not associated with the metadata. 
	 */
	public ECSMessage(ECSStatusType command, InfrastructureMetadata metadata) throws InvalidMessageException {
		LogSetup ls = new LogSetup("logs\\ecs.log", "ECS", Level.ALL);
		this.logger = ls.getLogger();
		
		if (command != ECSStatusType.INIT && command != ECSStatusType.UPDATE) {
			throw new InvalidMessageException("Incorrect number of arguments or command.");
		}
		this.command = command;
		this.metadata = metadata;
	}

	/**
	 * Construct a message with the moving data.
	 * @param command The type of the message.
	 * @param movingData The data that is being transfered from one server node to the other.
	 * @throws InvalidMessageException Thrown when the command is not associated with the movingData.
	 */
	public ECSMessage(ECSStatusType command, HashMap<BigInteger, String> movingData) throws InvalidMessageException {
		LogSetup ls = new LogSetup("logs\\ecs.log", "ECS", Level.ALL);
		this.logger = ls.getLogger();
		
		if (command != ECSStatusType.MOVE_DATA_INTERNAL) {
			throw new InvalidMessageException("Incorrect number of arguments or command.");
		}
		this.command = command;
		this.movingData = movingData;
	}

	/**
	 * Construct a message with a range index and the server data.
	 * @param command The type of the message.
	 * @param startIndex The first index the server serves.
	 * @param endIndex The last index the server serves. It is the index of the server.
	 * @param server The server that the indexes correspond. 
	 * @throws InvalidMessageException Thrown when the command entered is correctly associated if less arguments. 
	 */
	public ECSMessage(ECSStatusType command, BigInteger startIndex, BigInteger endIndex, ServerData server) throws InvalidMessageException {
		LogSetup ls = new LogSetup("logs\\ecs.log", "ECS", Level.ALL);
		this.logger = ls.getLogger();
		
		if (command != ECSStatusType.MOVE_DATA) {
			throw new InvalidMessageException("Incorrect number of arguments or command.");
		}
		this.command = command;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.server = server;
	}

	/**
	 * Transform the message in an array of bytes to be sent to the ECS or to a server
	 * @return The message in an array of bytes.
	 */
	public byte[] toBytes() {
		byte[] bytes = null;
		String message = command.toString();

		if (command == ECSStatusType.START || command == ECSStatusType.STOP
				|| command == ECSStatusType.SHUTDOWN || command == ECSStatusType.LOCK_WRITE
				|| command == ECSStatusType.UNLOCK_WRITE || command == ECSStatusType.MOVE_COMPLETED
				|| command == ECSStatusType.MOVE_ERROR || command == ECSStatusType.MOVE_DATA_INTERNAL_SUCCESS
				|| command == ECSStatusType.FAILED || command == ECSStatusType.GET_STATUS) {
			message += "\r";
		} else if (command == ECSStatusType.INIT || command == ECSStatusType.UPDATE) {
			message += "\n" + metadata.toString() + "\r";
		} else if (command == ECSStatusType.MOVE_DATA_INTERNAL) {
			message += "\n" + getData() + "\r";
		} else if (command == ECSStatusType.MOVE_DATA) {
			message += "\n" + startIndex.toString() + "\n" + endIndex.toString() + "\n"
					+ server.getName() + "\n" + server.getAddress() + "\n" 
					+ Integer.toString(server.getPort()) + "\r";
		} else {
			logger.error("Cannot convert KVQuery to bytes, since it has an incorrect number of arguments.");
			return null;
		}

		bytes = message.getBytes();
		if (bytes.length > KVMessage.DROP_SIZE) {
			logger.error("Cannot convert KVQuery to bytes, since the payload would be too large.\n"
					+ "  Payload: " + bytes.length / 1024 + " kb"
					+ "  Maxmium allowed: " + KVMessage.DROP_SIZE / 1024 + " kb");
			return null;
		}

		return bytes;
	}

	/**
	 * Get the command of the message.
	 * @return The command of the message.
	 */
	public ECSStatusType getCommand() {
		return command;
	}

	/**
	 * Get the metadata associated with the message if the command represents a message with a metadata argument. 
	 * @return The metadata associated with the message.
	 * @throws InvalidMessageException Thrown when the command is not associated with a metadata argument.
	 */
	public InfrastructureMetadata getMetadata() throws InvalidMessageException {
		if (command != ECSStatusType.INIT && command != ECSStatusType.UPDATE) {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
		return metadata;
	}

	/**
	 * Get the start index associated with the message if the command represents a message with a start index argument. 
	 * @return The start index of the server associated with the message.
	 * @throws InvalidMessageException Thrown when the command is not associated with a start index argument.
	 */
	public BigInteger getStartIndex() throws InvalidMessageException {
		if (command != ECSStatusType.MOVE_DATA) {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
		return startIndex;
	}

	/**
	 * Get the end index associated with the message if the command represents a message with an end index argument. 
	 * @return The end index of the server associated with the message.
	 * @throws InvalidMessageException Thrown when the command is not associated with an end index argument.
	 */
	public BigInteger getEndIndex() throws InvalidMessageException {
		if (command != ECSStatusType.MOVE_DATA) {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
		return endIndex;
	}

	/**
	 * Get the server associated with the message if the command represents a message with a server argument. 
	 * @return The server associated with the message. The index is the same as the server index.
	 * @throws InvalidMessageException Thrown when the command is not associated with a server argument.
	 */
	public ServerData getServer() throws InvalidMessageException {
		if (command != ECSStatusType.MOVE_DATA) {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
		return server;
	}

	/**
	 * Get the data the is being moved from one server to another.
	 * @return The data the is being moved from one server to another.
	 * @throws InvalidMessageException Thrown when the command is not associated with a moving data argument.
	 */
	public HashMap<BigInteger, String> getMovingData() throws InvalidMessageException {
		if (command != ECSStatusType.MOVE_DATA_INTERNAL) {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
		return movingData;
	}

	private String getData() {
		StringBuilder data = new StringBuilder();
		if (movingData.isEmpty()) {
			return movingDataEmpty;
		} else {
			for (Entry<BigInteger, String> entry : movingData.entrySet()) {
				data.append(entry.getKey().toString() + "," + entry.getValue() + ";");
			}
		}
		return data.toString();
	}

	private void createMovingData(String movingData) {
		this.movingData = new HashMap<BigInteger, String>();
		if (!movingData.equals(movingDataEmpty)) {
			String[] data = movingData.split(";");
			for (String dataStr : data) {
				String[] dataEntry = dataStr.split(",");
				this.movingData.put(new BigInteger(dataEntry[0]), dataEntry[1]);
			}
		}
	}

	private void setType(String command) throws InvalidMessageException {
		try {
			this.command = ECSStatusType.valueOf(command);
		} catch (Exception ex) {
			throw new InvalidMessageException("This code does not represent a command.");        
		}
	}
}
