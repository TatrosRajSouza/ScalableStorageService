package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.Settings;

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
 	private String moduleName = "<KVComm Module>";
 	private static final int TIMEOUT_MS = 3000;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 4096 * BUFFER_SIZE;
	
	/**
	 * Initializes communication by establishing a connection to the given address and port
	 * @param address
	 * @param port
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public KVCommunication(String address, int port, String name) throws UnknownHostException, IOException {
		initLog();
		connect(address, port);
	}
	
	public void initLog() {
		LogSetup ls = new LogSetup("logs/client.log", this.moduleName, Level.ALL);
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
			logger.info(" Connection established");
	}
	
	/**
	 * Gracefully closes the connection to the KVServer.
	 */
	public void closeConnection() {
		if(KVClient.DEBUG)
			logger.info(" try to close connection ...");
		
		try {
			tearDownConnection();
			setSocketStatus(SocketStatus.DISCONNECTED);
		} catch (IOException ioe) {
			if(KVClient.DEBUG)
				logger.error(" Unable to close connection!");
		}
	}
	
	/**
	 * Closes socket and streams.
	 * @throws IOException
	 */
	private void tearDownConnection() throws IOException {
		if(KVClient.DEBUG)
			logger.info(" tearing down the connection ...");
		
		if (clientSocket != null) {
			if (input != null)
				input.close();
			if (output != null)
				output.close();
			
			clientSocket.close();
			clientSocket = null;
			
			if(KVClient.DEBUG)
				logger.info(" Connection closed by communication module!");
		}
	}
	
	/**
	 * Method sends a Message using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	private void sendMessageInternal(byte[] msgBytes, int ident) throws IOException, SocketTimeoutException {
		if (msgBytes != null) {
			output = clientSocket.getOutputStream();
			byte[] bytes = ByteBuffer.allocate(8 + msgBytes.length).putInt(ident).putInt(msgBytes.length).put(msgBytes).array();
			output.write(bytes, 0, bytes.length);
			output.flush();
			logger.debug("Sent to           [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " + new String(bytes, Settings.CHARSET));
			/*
			if(KVClient.DEBUG) {
				logger.info(" SEND \t<" 
						+ clientSocket.getInetAddress().getHostAddress() + ":" 
						+ clientSocket.getPort() + ">: '" 
						+ new String(msgBytes) +"'");
			}
			*/
		} else {
			throw new IOException(" Unable to transmit message, the message was null.");
		}
	}
	
	/**
	 * Method sends a Message using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(byte[] msgBytes) throws IOException, SocketTimeoutException {
		sendMessageInternal(msgBytes, 1);
	}
	
	/**
	 * Method sends a Message using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessageECS(byte[] msgBytes) throws IOException, SocketTimeoutException {
		sendMessageInternal(msgBytes, 3);
	}
	
	public byte[] receiveMessage() throws IOException, SocketTimeoutException {
		input = clientSocket.getInputStream();
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read message identity */
		byte[] identBytes = new byte[4];
		int bytesRead = input.read(identBytes);
		int ident = ByteBuffer.wrap(identBytes).getInt(); // 1 = CLIENT, 2 = SERVER, 3 = ECS
		/* read length of message */
		byte[] lenBytes = new byte[4];
		bytesRead = input.read(lenBytes);
		int msgLen = ByteBuffer.wrap(lenBytes).getInt();
		logger.debug("new message - length: " + msgLen + ", ident: " + ident);

		for (int i = 0; i < msgLen; i++) {
			/* read next byte */
			byte read = (byte) input.read();
			
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
			/*
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}*/
		}
		// logger.debug("DONE READING.");

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
		/*
		if(KVClient.DEBUG) {
			logger.info(" RECEIVE \t<" 
					+ clientSocket.getInetAddress().getHostAddress() + ":" 
					+ clientSocket.getPort() + ">: '" 
					+ new String(msgBytes) + "'");
		}
		*/
		logger.debug("Received from     [" + this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort() + "] " + "RAW DATA\n<" + new String(msgBytes, Settings.CHARSET) + ">");
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
	
	public Socket getSocket() {
		return this.clientSocket;
	}
}
