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
	private byte[] IV;
	boolean requireClientAuth;

	public ServerAuthConfirmationMessage(byte[] secureHash, byte[] IV, boolean requireClientAuth) {
		this.requireClientAuth = requireClientAuth;
		this.confirmationHash = secureHash;
		this.IV = IV;
	}
	
	public byte[] getConfirmationHash() {
		return this.confirmationHash;
	}
	
	public byte[] getIV() {
		return this.IV;
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
			sb.append(", SecureConfirmationHash: <ENCODING_NOT_SUPPORTED>");
		}
		
		try {
			sb.append("\n-----BEGIN IV-----\n");
			sb.append(new String(this.IV, Settings.CHARSET));
			sb.append("\n-----END IV-----");
		} catch (UnsupportedEncodingException e) {
			sb.append(", IV: <ENCODING_NOT_SUPPORTED>");
		}
		
		return sb.toString();
	}
}