package common.messages;

import java.math.BigInteger;

import org.apache.log4j.Logger;

public class ECSMessage {

	private ECSStatusType command;
	private InfrastructureMetadata metadata;
	private BigInteger startIndex;
	private BigInteger endIndex;
	private ServerData server;
	private String message;

	private static Logger logger = Logger.getRootLogger();

	public ECSMessage(byte[] bytes) throws InvalidMessageException {
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
				metadata = null;
				startIndex = null;
				endIndex = null;
				server = null;
				break;
			case 2:
				if (command == ECSStatusType.INIT || command == ECSStatusType.UPDATE) {
					metadata = new InfrastructureMetadata(arguments[1]);
				} else if (command == ECSStatusType.CONNECT_ERROR) {
					this.message = arguments[1];
				} else {
					throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
				}
				startIndex = null;
				endIndex = null;
				server = null;
				break;
			case 6:
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

	public ECSMessage(ECSStatusType command)
			throws InvalidMessageException {
		if (command == ECSStatusType.INIT || command == ECSStatusType.UPDATE
				|| command == ECSStatusType.MOVE_DATA || command == ECSStatusType.CONNECT_ERROR) {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
		this.command = command;
	}

	public ECSMessage(ECSStatusType command, InfrastructureMetadata metadata)
			throws InvalidMessageException {
		if (command != ECSStatusType.INIT && command != ECSStatusType.UPDATE) {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
		this.command = command;
		this.metadata = metadata;
	}

	public ECSMessage(ECSStatusType command, String message)
			throws InvalidMessageException {
		if (command != ECSStatusType.CONNECT_ERROR) {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
		this.message = message;
		this.command = command;
	}

	public ECSMessage(ECSStatusType command, BigInteger startIndex, BigInteger endIndex, ServerData server)
			throws InvalidMessageException {
		if (command != ECSStatusType.MOVE_DATA) {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
		this.command = command;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.server = server;
	}

	public byte[] toBytes() {
		byte[] bytes = null;
		String message = command.toString();

		if (command == ECSStatusType.START || command == ECSStatusType.STOP
				|| command == ECSStatusType.SHUTDOWN || command == ECSStatusType.LOCK_WRITE
				|| command == ECSStatusType.UNLOCK_WRITE || command == ECSStatusType.CONNECT
				|| command == ECSStatusType.CONNECT_SUCCESS || command == ECSStatusType.DISCONNECT
				|| command == ECSStatusType.DISCONNECT_SUCCESS) {
			message += "\r";
		} else if (command == ECSStatusType.INIT || command == ECSStatusType.UPDATE) {
			message += "\n" + metadata.toString() + "\r";
		} else if (command == ECSStatusType.CONNECT_ERROR) {
			message += "\n" + this.message + "\r";
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

	public ECSStatusType getCommand() {
		return command;
	}

	public InfrastructureMetadata getMetadata() throws InvalidMessageException {
		if (command != ECSStatusType.INIT && command != ECSStatusType.UPDATE) {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
		return metadata;
	}

	public BigInteger getStartIndex() throws InvalidMessageException {
		if (command != ECSStatusType.MOVE_DATA) {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
		return startIndex;
	}

	public BigInteger getEndIndex() throws InvalidMessageException {
		if (command != ECSStatusType.MOVE_DATA) {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
		return endIndex;
	}

	public ServerData getServer() throws InvalidMessageException {
		if (command != ECSStatusType.MOVE_DATA) {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
		return server;
	}

	public String getMessage() throws InvalidMessageException {
		if (command != ECSStatusType.CONNECT_ERROR) {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
		return message;
	}

	private void setType(String command) throws InvalidMessageException {
		try {
			this.command = ECSStatusType.valueOf(command);
		} catch (Exception ex) {
			throw new InvalidMessageException("This code does not represent a command.");        
		}
	}
}
