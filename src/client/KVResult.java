package client;

import common.messages.KVMessage;

/**
 * Implements the KVMessage Interface, giving users of the library
 * access to retrieved information, such as the received message type, keys and their values. 
 * @author Elias Tatros
 *
 */
public class KVResult implements KVMessage {
	StatusType status;
	String key;
	String value;
	
	public KVResult(StatusType status, String key, String value) {
		this.status = status;
		this.key = key;
		this.value = value;
	}
	
	@Override
	public String getKey() {
		return this.key;
	}
	
	@Override
	public String getValue() {
		return this.value;
	}
	
	@Override
	public StatusType getStatus() {
		return this.status;
	}
}
