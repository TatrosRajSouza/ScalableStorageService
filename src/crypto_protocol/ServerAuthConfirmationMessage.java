package crypto_protocol;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;

import common.Settings;

/**
 * Final confirmation Message sent by server.
 * Contains HMAC of all message contents up to this point.
 * @author Elias Tatros
 *
 */
public class ServerAuthConfirmationMessage implements Message, Serializable {
	
	private static final long serialVersionUID = -2205848968909155801L;
	public static final MessageType MESSAGE_TYPE = MessageType.ServerAuthConfirmationMessage;
	private byte[] confirmationHash;
	boolean requireClientAuth;

	public ServerAuthConfirmationMessage(byte[] secureHash, boolean requireClientAuth) {
		this.requireClientAuth = requireClientAuth;
		this.confirmationHash = secureHash;
	}
	
	
	public byte[] getConfirmationHash() {
		return this.confirmationHash;
	}
	
	
	public MessageType getType() {
		return MESSAGE_TYPE;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Type: " + MESSAGE_TYPE);
		sb.append(", RequireClientAuth: " + this.requireClientAuth);
		
		try {
			sb.append("\n-----BEGIN SECURE CONFIRMATION HASH-----\n");
			sb.append(new String(this.confirmationHash, Settings.CHARSET));
			sb.append("\n-----END SECURE CONFIRMATION HASH-----");
		} catch (UnsupportedEncodingException e) {
			sb.append(", Nonce: <ENCODING_NOT_SUPPORTED>");
		}
		
		return sb.toString();
	}
}