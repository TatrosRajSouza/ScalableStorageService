package common.messages;

import java.math.BigInteger;

import org.apache.log4j.Logger;

public class ECSMessage {

	private ECSStatusType command;
	private InfrastructureMetadata metadata;
	private BigInteger index;
	private ServerData server;

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
				index = null;
				server = null;
				break;
			case 2:
				metadata = new InfrastructureMetadata(arguments[1]);
				index = null;
				server = null;
				break;
			case 5:
				metadata = null;
				index = new BigInteger(arguments[1]);
				server = new ServerData(arguments[2], arguments[3], Integer.parseInt(arguments[4]));
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
				|| command == ECSStatusType.MOVE_DATA || command == ECSStatusType.MOVE_COMPLETED) {
			throw new InvalidMessageException("Incorrect number of arguments or command.");
		}
		this.command = command;
	}

	public ECSMessage(ECSStatusType command, InfrastructureMetadata metadata)
			throws InvalidMessageException {
		if (command != ECSStatusType.INIT && command != ECSStatusType.UPDATE) {
			throw new InvalidMessageException("Incorrect number of arguments or command.");
		}
		this.command = command;
		this.metadata = metadata;
	}

	public ECSMessage(ECSStatusType command, BigInteger startIndex, ServerData server)
			throws InvalidMessageException {
		if (command != ECSStatusType.MOVE_DATA) {
			throw new InvalidMessageException("Incorrect number of arguments or command.");
		}
		this.command = command;
		this.index = startIndex;
		this.server = server;
	}

	public byte[] toBytes() {
		byte[] bytes = null;
		String message = command.toString();

		if (command == ECSStatusType.START || command == ECSStatusType.STOP
				|| command == ECSStatusType.SHUTDOWN || command == ECSStatusType.LOCK_WRITE
				|| command == ECSStatusType.UNLOCK_WRITE || command == ECSStatusType.MOVE_COMPLETED) {
			message += "\r";
		} else if (command == ECSStatusType.INIT || command == ECSStatusType.UPDATE) {
			message += "\n" + metadata.toString() + "\r";
		} else if (command == ECSStatusType.MOVE_DATA) {
			message += "\n" + index.toString() + "\n"
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

	public BigInteger getIndex() throws InvalidMessageException {
		if (command != ECSStatusType.MOVE_DATA) {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
		return index;
	}

	public ServerData getServer() throws InvalidMessageException {
		if (command != ECSStatusType.MOVE_DATA) {
			throw new InvalidMessageException("Incorrect number of arguments or unknown command.");
		}
		return server;
	}

	private void setType(String command) throws InvalidMessageException {
		try {
			this.command = ECSStatusType.valueOf(command);
		} catch (Exception ex) {
			throw new InvalidMessageException("This code does not represent a command.");        
		}
	}
}
