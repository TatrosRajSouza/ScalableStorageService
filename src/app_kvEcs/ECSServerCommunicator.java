package app_kvEcs;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import client.KVCommunication;
import common.messages.ECSMessage;
import common.messages.InvalidMessageException;
import common.messages.ServerData;

public class ECSServerCommunicator extends ServerData {
	KVCommunication communication;
	
	public ECSServerCommunicator(String name, String address, int port) {
		super(name, address, port);
		communication = null;
	}
	
	public void connect() throws UnknownHostException, IOException {
		try {
			communication = new KVCommunication(getAddress(), getPort());
		} catch (UnknownHostException e) {
			ECS.logger.error("Error! Couldn't connect to " + getAddress() + ":" + getPort());
		} catch (IOException e) {
			try {
				Thread.sleep(200);
			} catch (InterruptedException e1) {
				ECS.logger.warn("Warn! Thread interrupted by other thread.");
			}
			connect();
		}
	}
	
	public void disconnect() {
		communication.closeConnection();
	}

	public void sendMessage(byte[] msgBytes) throws SocketTimeoutException, IOException {
		communication.sendMessage(msgBytes);
	}
	
	public ECSMessage receiveMessage() throws SocketTimeoutException, IOException, InvalidMessageException {
		byte[] message = communication.receiveMessage();
		return new ECSMessage(message);
	}
}
