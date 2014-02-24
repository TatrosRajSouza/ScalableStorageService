package crypto_protocol;

import java.io.Serializable;

public class ErrorMessage implements Message, Serializable {

	private static final long serialVersionUID = 8735388401537254786L;
	
	public static final MessageType MESSAGE_TYPE = MessageType.ErrorMessage;
	private String message;
	
	public ErrorMessage(String message) {
		this.message = message;
	}
	
	public String getMessage() {
		return this.message;
	}
	
	public MessageType getType() {
		return MESSAGE_TYPE;
	}
	
	@Override
	public String toString() {
		return this.message;
	}
}
