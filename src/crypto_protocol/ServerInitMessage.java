package crypto_protocol;

import java.io.File;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import common.Settings;

/**
 * First Message sent by Server during handshake.
 * Contains the Servers random nonce, used cipher suite, server certificate
 * and the authentication type (simple or mutual)
 * @author Elias Tatros
 *
 */
public class ServerInitMessage implements Message, Serializable {

	private static final long serialVersionUID = -237671777951765695L;

	public static final MessageType MESSAGE_TYPE = MessageType.ServerInitMessage;
	private final int nonceLength = 28; // length of nonce in bytes
	private byte[] serverNonce; // contains randomly generated nonce
	private List<String> supportedCiphers = Arrays.asList(Settings.TRANSFER_ENCRYPTION);
	private String cipherSuite = "";
	private boolean requireClientAuth;
	private transient File certificateFile;
	X509Certificate serverCertificate;
	
	public ServerInitMessage(String cipherSuite, X509Certificate serverCertificate, boolean requireClientAuth) {
		this.serverNonce = new byte[nonceLength];
		this.requireClientAuth = requireClientAuth;
		this.serverCertificate = serverCertificate;
		
		if (supportedCiphers.contains(cipherSuite)) {
			this.cipherSuite = cipherSuite;
			
			Random rand = new Random();
			rand.nextBytes(this.serverNonce);
		} else {
			throw new IllegalArgumentException("The cipher suite <" + cipherSuite + "> is not supported.");
		}
	}
	

	
	public MessageType getType() {
		return MESSAGE_TYPE;
	}
	
	public byte[] getNonce() {
		return this.serverNonce;
	}
	
	public String getCipherSuite() {
		return this.cipherSuite;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Type: " + MESSAGE_TYPE);
		sb.append(", CipherSuite: " + cipherSuite);
		sb.append(", RequireClientAuth: " + requireClientAuth);
		
		if (this.certificateFile != null)
			sb.append(", Cert File: " + this.certificateFile.getPath());
		
		sb.append(", Cert.Subject: " + this.serverCertificate.getSubjectX500Principal());
		
		try {
			sb.append(", Nonce: " + new String(serverNonce, Settings.CHARSET));
		} catch (UnsupportedEncodingException e) {
			sb.append(", Nonce: <ENCODING_NOT_SUPPORTED>");
		}
		
		return sb.toString();
	}
	
	public X509Certificate getCertificate() {
		return this.serverCertificate;
	}
	
	public boolean isClientAuthRequired() {
		return this.requireClientAuth;
	}

}
