package crypto_protocol;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.cert.X509Certificate;

import common.Settings;

/**
 * Second message sent by client during handshake.
 * Contains the pre-master secret, encrypted with the Servers public key,
 * HMAC over all messages up to this point and
 * optionally a client certificate and Signature
 * @author Elias Tatros
 *
 */
public class ClientKeyExchangeMessage implements Message, Serializable {
	
	public static final MessageType MESSAGE_TYPE = MessageType.ClientKeyExchangeMessage;
	
	boolean requireClientAuth;
	byte[] encryptedSecret = null;
	byte[] secureSessionHash = null;
	
	byte[] signature = null;
	X509Certificate clientCertificate = null;
	
	private static final long serialVersionUID = 4943771340123613732L;

	/** 
	 * Simple Constructor
	 * @param encryptedSecret
	 * @param secureSessionHash
	 */
	public ClientKeyExchangeMessage(byte[] encryptedSecret, byte[] secureSessionHash) {
		this.requireClientAuth = false;
		this.encryptedSecret = encryptedSecret;
		this.secureSessionHash = secureSessionHash;
	}
	
	
	/**
	 * Constructor including Client Authentication
	 * @param encryptedSecret
	 * @param secureSessionHash
	 * @param clientCertificate
	 * @param signature
	 */
	public ClientKeyExchangeMessage(byte[] encryptedSecret, byte[] secureSessionHash, X509Certificate clientCertificate, byte[] signature) {
		this.requireClientAuth = true;
		this.encryptedSecret = encryptedSecret;
		this.secureSessionHash = secureSessionHash;
		this.clientCertificate = clientCertificate;
		this.signature = signature;
	}
	
	public byte[] getEncryptedSecret() {
		return this.encryptedSecret;
	}
	
	public byte[] getSecureSessionHash() {
		return this.secureSessionHash;
	}
	
	public byte[] getSignature() {
		return this.signature;
	}
	
	public X509Certificate getClientCertificate() {
		return this.clientCertificate;
	}
	
	public boolean isClientAuthRequired() {
		return this.requireClientAuth;
	}
	
	public MessageType getType() {
		return MESSAGE_TYPE;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Type: " + MESSAGE_TYPE);
		sb.append(", CipherSuite: " + Settings.ALGORITHM_ENCRYPTION);
		sb.append(", RequireClientAuth: " + this.requireClientAuth);
		
		if (this.requireClientAuth && (this.clientCertificate != null)) {
			sb.append(", ClientCert: " + this.clientCertificate.getSubjectX500Principal().getName());
		
			try {
				sb.append("\n-----BEGIN CLIENT SIGNATURE-----\n");
				sb.append(new String(this.signature, Settings.CHARSET));
				sb.append("\n-----END CLIENT SIGNATURE-----\n");
			} catch (UnsupportedEncodingException e) {
				sb.append("\n-----BEGIN SIGNED SERVER NONCE-----\n");
				sb.append("Signed Server Nonce: <ENCODING_NOT_SUPPORTED>");
				sb.append("\n-----END SIGNED SERVER NONCE-----\n");
			}
		}
		
		try {
			sb.append("\n-----BEGIN SECURE SESSION HASH-----\n");
			sb.append(new String(this.secureSessionHash, Settings.CHARSET));
			sb.append("\n-----END SECURE SESSION HASH-----\n");
		} catch (UnsupportedEncodingException e) {
			sb.append("\n-----BEGIN SECURE SESSION HASH-----\n");
			sb.append("Secure Session Hash: <ENCODING_NOT_SUPPORTED>");
			sb.append("\n-----END SECURE SESSION HASH-----\n");
		}
		
		try {
			sb.append("\n-----BEGIN ENCRYPTED SECRET-----\n");
			sb.append(new String(this.encryptedSecret, Settings.CHARSET));
			sb.append("\n-----END ENCRYPTED SECRET-----");
		} catch (UnsupportedEncodingException e) {
			sb.append("\n-----BEGIN ENCRYPTED SECRET-----\n");
			sb.append("Encrypted Secret: <ENCODING_NOT_SUPPORTED>");
			sb.append("\n-----END ENCRYPTED SECRET-----");
		}
		
		return sb.toString();
	}
}
