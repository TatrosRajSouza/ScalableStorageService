package common;

/**
 * Global settings
 * @author Elias Tatros
 *
 */
public final class Settings {
	public static final String ALGORITHM_HASHING = "HmacSHA1"; // Secure hashing algorithm
	public static final String ALGORITHM_ENCRYPTION = "RSA"; // Encryption used during authentication
	public static final String TRANSFER_ENCRYPTION = "AES/CBC/PKCS5Padding"; //Symmetric file transfer encryption
	public static final String CHARSET = "UTF-8";
	public static String PAYLOAD_FILE = "payload.txt";
	public static byte[] IV;
	public static boolean USE_CLIENT_AUTH = true;
	
	public static String SERVER_CERT_PATH =  "certs/serverCert/serverCert.pem";
	public static String SERVER_PRIVKEY_PATH = "certs/serverCert/serverKey.pem";
	public static String CA_CERT_PATH = "certs/CACert.pem";
	public static final String TRUSTED_CA_PATH = "certs/trustStore/";
	public static final String SERVER_FILES = "serverFiles/";
	public static final String CLIENT_FILES = "clientFiles/";
	
	public static String getCACertPath() {
		 return Settings.CA_CERT_PATH;
	}
	
	public static void setCACertPath(String value) {
		Settings.CA_CERT_PATH = value;
	}
	
	public static byte[] endSequence = "END_OF_MESSAGE".getBytes();
}
