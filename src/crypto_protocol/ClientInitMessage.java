package crypto_protocol;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Random;

import common.Settings;

/**
 * Initial Message sent by the Client
 * Contains the client nonce and chosen cipher suite.
 * @author Elias Tatros
 *
 */
public class ClientInitMessage implements Message, Serializable {

	private static final long serialVersionUID = 4753270788056217548L;

	/* Type of this message */
	public static final MessageType MESSAGE_TYPE = MessageType.ClientInitMessage;
	/* Length of nonce in bytes */
	private final int nonceLength = 28;
	/* Randomly generated nonce */
	private byte[] clientNonce;
	/* Chosen cipher suite */
	private String cipherSuite = Settings.TRANSFER_ENCRYPTION;
	
	public ClientInitMessage() {
		/* Generate Nonce */
		this.clientNonce = new byte[nonceLength];
		
		Random rand = new Random();
		rand.nextBytes(this.clientNonce);
	}
	
	public MessageType getType() {
		return MESSAGE_TYPE;
	}
	
	public byte[] getNonce() {
		return this.clientNonce;
	}
	
	public String getCipherSuite() {
		return this.cipherSuite;
	}
	
	@Override
	public String toString() {
		try {
			return new String("Type: " + MESSAGE_TYPE + ", cipherSuite: " + cipherSuite + ", nonce: " + new String(clientNonce, Settings.CHARSET));
		} catch (UnsupportedEncodingException e) {
			return new String("Type: " + MESSAGE_TYPE + ", cipherSuite: " + cipherSuite + ", nonce: " + "<ENCODING_NOT_SUPPORTED> " + e.getMessage());
		} 
	}
}
