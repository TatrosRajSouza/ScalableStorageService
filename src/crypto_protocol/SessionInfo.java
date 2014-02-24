package crypto_protocol;

import java.io.UnsupportedEncodingException;
import java.security.cert.X509Certificate;

import javax.crypto.spec.SecretKeySpec;

import common.Settings;

/**
 * Represents a session between client and server.
 * Tracks & verifies all important session information.
 * @author Elias Tatros
 *
 */
public class SessionInfo {
	private String clientIP;
	private String serverIP;
	private int localPort;
	private int remotePort;
	
	private SecretKeySpec encKey;
	private SecretKeySpec macKey;
	private byte[] secureSessionHash;
	private byte[] secureConfirmationHash;
	private byte[] masterSecret;
	private byte[] encryptedSecret;
	private byte[] clientNonce;
	private byte[] serverNonce;
	private boolean clientAuthRequired;
	private boolean isValid;

	X509Certificate serverCertificate;
	X509Certificate clientCertificate;
	X509Certificate caCertificate;

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\n=== SESSION INFO ===\n");
		
		try {
			sb.append("clientIP: " + this.clientIP);
			sb.append("\nserverIP: " + this.serverIP);
			sb.append("\nlocalPort: " + this.localPort);
			sb.append("\nremotePort: " + this.remotePort);
			
			if (encKey != null)
				sb.append("\nencKey: " + new String(encKey.getEncoded(), Settings.CHARSET));
			else
				sb.append("\nencKey: " + "<NOT SET>");
			
			if (macKey != null)
				sb.append("\nmacKey: " + new String(macKey.getEncoded(), Settings.CHARSET));
			else
				sb.append("\nmacKey: " + "<NOT SET>");
			
			if (secureSessionHash != null)
				sb.append("\nsessionHash: " + new String(secureSessionHash, Settings.CHARSET));
			else
				sb.append("\nsessionHash: " + "<NOT SET>");
			
			if (secureConfirmationHash != null)
				sb.append("\nconfirmationHash: " + new String(secureConfirmationHash, Settings.CHARSET));
			else
				sb.append("\nconfirmationHash: " + "<NOT SET>");
			
			if (masterSecret != null)
				sb.append("\nmasterSecret: " + new String(masterSecret, Settings.CHARSET));
			else
				sb.append("\nmasterSecret: " + "<NOT SET>");
			
			if (encryptedSecret != null)
				sb.append("\nencryptedSecret: " + new String(encryptedSecret, Settings.CHARSET));
			else
				sb.append("\nencryptedSecret: " + "<NOT SET>");
			
			if (clientNonce != null)
				sb.append("\nclientNonce: " + new String(clientNonce, Settings.CHARSET));
			else
				sb.append("\nclientNonce: " + "<NOT SET>");
			
			if (serverNonce != null)
				sb.append("\nserverNonce: " + new String(serverNonce, Settings.CHARSET));
			else
				sb.append("\nserverNonce: " + "<NOT SET>");
			
			if (serverCertificate != null)
				sb.append("\nserverCertificate: " + serverCertificate);
			else
				sb.append("\nserverCertificate: " + "<NOT SET>");
			
			if (clientCertificate != null)
				sb.append("\nclientCertificate: " + clientCertificate);
			else
				sb.append("\nclientCertificate: " + "<NOT SET>");
			
			if (caCertificate != null)
				sb.append("\ncaCertificate: " + caCertificate);
			else
				sb.append("\ncaCertificate: " + "<NOT SET>");
		} catch (UnsupportedEncodingException ex) {
			sb.append("ENCODING NOT SUPPORTED:\n" + ex.getMessage());
		}
		sb.append("\n____________________");
		return sb.toString();
	}

	public SessionInfo() {
		this.clientAuthRequired = false;
		
		this.encKey = null;
		this.macKey = null;
		this.secureSessionHash = null;
		this.secureConfirmationHash = null;
		this.masterSecret = null;
		this.encryptedSecret = null;
		this.clientNonce = null;
		this.serverNonce = null;
		
		this.serverCertificate = null;
		this.clientCertificate = null;
		
		this.isValid = false;
	}
	
	public void validateSession() throws SessionException {
		if (this.encKey == null)
			throw new SessionException("Session invalid. Encryption Key undefined.");
		
		if (this.macKey == null)
			throw new SessionException("Session invalid. HMAC Key undefined.");
		
		if (this.secureSessionHash == null)
			throw new SessionException("Session invalid. Secure Session Hash undefined.");
		
		if (this.secureConfirmationHash == null)
			throw new SessionException("Session invalid. Secure Confirmation Hash undefined.");
		
		if (this.masterSecret == null)
			throw new SessionException("Session invalid. Master Secret undefined.");
		
		if (this.encryptedSecret == null)
			throw new SessionException("Session invalid. Encrypted Secret undefined.");
		
		if (this.clientNonce == null)
			throw new SessionException("Session invalid. Client Nonce undefined.");
		
		if (this.serverNonce == null)
			throw new SessionException("Session invalid. Server Nonce undefined.");
		
		if (this.serverCertificate == null)
			throw new SessionException("Session invalid. Server Certificate undefined.");
		
		if (isClientAuthRequired()) {
			if (this.clientCertificate == null)
				throw new SessionException("Session invalid. Client Certificate undefined.");
		}
		
		isValid = true;
	}
	
	public boolean isValid() {
		return isValid;
	}
	
	public X509Certificate getCACertificate() {
		return caCertificate;
	}

	public void setCACertificate(X509Certificate caCertificate) {
		if (caCertificate == null) {
			throw new IllegalArgumentException("Invalid value for caCertificate. (null)");
		}
		
		this.caCertificate = caCertificate;
	}
	
	public String getClientIP() {
		return clientIP;
	}

	public void setClientIP(String clientIP) {
		this.clientIP = clientIP;
	}

	public String getServerIP() {
		return serverIP;
	}

	public void setServerIP(String serverIP) {
		this.serverIP = serverIP;
	}

	public int getLocalPort() {
		return localPort;
	}

	public void setLocalPort(int localPort) {
		this.localPort = localPort;
	}

	public int getRemotePort() {
		return remotePort;
	}

	public void setRemotePort(int remotePort) {
		this.remotePort = remotePort;
	}
	
	public X509Certificate getServerCertificate() {
		return serverCertificate;
	}

	public void setServerCertificate(X509Certificate serverCertificate) {
		if (serverCertificate == null) {
			throw new IllegalArgumentException("Invalid value for serverCertificate. (null)");
		}
		
		this.serverCertificate = serverCertificate;
	}

	public X509Certificate getClientCertificate() {
		return clientCertificate;
	}

	public void setClientCertificate(X509Certificate clientCertificate) {
		if (isClientAuthRequired() && clientCertificate == null) {
			throw new IllegalArgumentException("Invalid value for clientCertificate. (null)");
		}
		
		this.clientCertificate = clientCertificate;
	}
	
	public byte[] getClientNonce() {
		return clientNonce;
	}

	public void setClientNonce(byte[] clientNonce) {
		if (clientNonce == null) {
			throw new IllegalArgumentException("Invalid value for clientNonce. (null)");
		}
		
		this.clientNonce = clientNonce;
	}

	public byte[] getServerNonce() {
		return serverNonce;
	}

	public void setServerNonce(byte[] serverNonce) {
		if (serverNonce == null) {
			throw new IllegalArgumentException("Invalid value for serverNonce. (null)");
		}
		
		this.serverNonce = serverNonce;
	}
	
	public byte[] getMasterSecret() {
		return masterSecret;
	}

	public void setMasterSecret(byte[] masterSecret) {
		if (masterSecret == null) {
			throw new IllegalArgumentException("Invalid value for masterSecret. (null)");
		}
		
		this.masterSecret = masterSecret;
	}
	
	public byte[] getEncryptedSecret() {
		return encryptedSecret;
	}

	public void setEncryptedSecret(byte[] encryptedSecret) {
		if (encryptedSecret == null) {
			throw new IllegalArgumentException("Invalid value for encryptedSecret. (null)");
		}
		
		this.encryptedSecret = encryptedSecret;
	}

	public byte[] getSecureSessionHash() {
		return secureSessionHash;
	}

	public void setSecureSessionHash(byte[] secureSessionHash) {
		if (secureSessionHash == null) {
			throw new IllegalArgumentException("Invalid value for secureSessionHash. (null)");
		}
		
		this.secureSessionHash = secureSessionHash;
	}

	public byte[] getSecureConfirmationHash() {
		return secureConfirmationHash;
	}

	public void setSecureConfirmationHash(byte[] secureConfirmationHash) {
		if (secureConfirmationHash == null) {
			throw new IllegalArgumentException("Invalid value for secureConfirmationHash. (null)");
		}
		
		this.secureConfirmationHash = secureConfirmationHash;
	}
	
	public boolean isClientAuthRequired() {
		return this.clientAuthRequired;
	}

	public void setClientAuthRequired(boolean clientAuthRequired) {
		this.clientAuthRequired = clientAuthRequired;
	}

	public SecretKeySpec getEncKey() {
		return this.encKey;
	}
	
	public void setEncKey(SecretKeySpec value) {
		if (value == null) {
			throw new IllegalArgumentException("Invalid Encryption Key. (null)");
		}
		
		this.encKey = value;
	}
	
	public SecretKeySpec getMacKey() {
		return this.macKey;
	}
	
	public void setMacKey(SecretKeySpec value) {
		if (value == null) {
			throw new IllegalArgumentException("Invalid HMAC Key. (null)");
		}
		
		this.macKey = value;
	}
}
