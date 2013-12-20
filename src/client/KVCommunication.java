package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvClient.KVClient;
import app_kvClient.SocketStatus;

/**
 * Handles all basic communication logic used by the KVStore library. Holder of the communication socket.
 * @author Elias Tatros
 *
 */
public class KVCommunication {
	private Logger logger;
	private Socket clientSocket;
	private SocketStatus socketStatus;
	private OutputStream output;
 	private InputStream input;
 	private String name = "";
 	private String moduleName = "<KVComm Module>";
 	private static final int TIMEOUT_MS = 200;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	
	/**
	 * Initializes communication by establishing a connection to the given address and port
	 * @param address
	 * @param port
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public KVCommunication(String address, int port, String name) throws UnknownHostException, IOException {
		this.name = name;
		connect(address, port);
		initLog();
	}
	
	public void initLog() {
		LogSetup ls = new LogSetup("logs\\client.log", name, Level.ALL);
		this.logger = ls.getLogger();
	}
	
	/**
	 * Establishes socket connection to given address and port
	 * @param address the IPv4 address of the KVServer
	 * @param port valid port
	 * @throws UnknownHostException If the address cannot be resolved
	 * @throws IOException In case of communication error
	 * @throws SocketTimeoutException If the KVServer is unreachable.
	 */
	private void connect(String address, int port) throws UnknownHostException, IOException, SocketTimeoutException
	{
		clientSocket = new Socket(address, port);
		clientSocket.setSoTimeout(TIMEOUT_MS);
		setSocketStatus(SocketStatus.CONNECTED);
		
		if(KVClient.DEBUG)
			logger.info(this.name + moduleName + ": Connection established");
	}
	
	/**
	 * Gracefully closes the connection to the KVServer.
	 */
	public void closeConnection() {
		if(KVClient.DEBUG)
			logger.info(this.name + moduleName + ": try to close connection ...");
		
		try {
			tearDownConnection();
			setSocketStatus(SocketStatus.DISCONNECTED);
		} catch (IOException ioe) {
			if(KVClient.DEBUG)
				logger.error(this.name + moduleName + ": Unable to close connection!");
		}
	}
	
	/**
	 * Closes socket and streams.
	 * @throws IOException
	 */
	private void tearDownConnection() throws IOException {
		if(KVClient.DEBUG)
			logger.info(this.name + moduleName + ": tearing down the connection ...");
		
		if (clientSocket != null) {
			if (input != null)
				input.close();
			if (output != null)
				output.close();
			
			clientSocket.close();
			clientSocket = null;
			
			if(KVClient.DEBUG)
				logger.info(this.name + moduleName + ": Connection closed by communication module!");
		}
	}
	
	/**
	 * Method sends a Message using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(byte[] msgBytes) throws IOException, SocketTimeoutException {
		if (msgBytes != null) {
			output = clientSocket.getOutputStream();
			output.write(msgBytes, 0, msgBytes.length);
			output.flush();
			if(KVClient.DEBUG) {
				logger.info(this.name + moduleName + ": SEND \t<" 
						+ clientSocket.getInetAddress().getHostAddress() + ":" 
						+ clientSocket.getPort() + ">: '" 
						+ new String(msgBytes) +"'");
			}
		} else {
			throw new IOException(this.name + moduleName + ": Unable to transmit message, the message was null.");
		}
	}
	
	/**
	 * Receives a message as a byte array.
	 * @return the message as byte array
	 * @throws IOException In case of communication error
	 * @throws SocketTimeoutException If no message is received from the server within the timeout interval.
	 */
	public byte[] receiveMessage() throws IOException, SocketTimeoutException {
		input = clientSocket.getInputStream();
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;

		while(read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 

			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;

			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
		}

		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

		msgBytes = tmp;

		/* build final String */
		if(KVClient.DEBUG) {
			logger.info(this.name + moduleName + ": RECEIVE \t<" 
					+ clientSocket.getInetAddress().getHostAddress() + ":" 
					+ clientSocket.getPort() + ">: '" 
					+ new String(msgBytes) + "'");
		}
		return msgBytes;
	}

	/**
	 * Current status of the connection
	 * @return SocketStatus the status of the connection
	 */
	public SocketStatus getSocketStatus() {
		return socketStatus;
	}

	/**
	 * Internal use only. Sets the communication/socket status.
	 * @param socketStatus the status.
	 */
	private void setSocketStatus(SocketStatus socketStatus) {
		this.socketStatus = socketStatus;
	}
}
