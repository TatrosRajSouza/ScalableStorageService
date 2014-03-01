package common;

/**
 * Global settings
 * @author Elias Tatros
 *
 */
public final class Settings {
	static String fullpath = Settings
			.class
			.getProtectionDomain()
			.getCodeSource()
			.getLocation().getPath();
	static String home = fullpath.substring(0,fullpath.lastIndexOf("/"));
	public static final String ALGORITHM_HASHING = "HmacSHA1"; // Secure hashing algorithm
	public static final String ALGORITHM_ENCRYPTION = "RSA"; // Encryption used during authentication
	public static final String TRANSFER_ENCRYPTION = "AES/CBC/PKCS5Padding"; //Symmetric file transfer encryption
	public static final String CHARSET = "UTF-8";
	public static String PAYLOAD_FILE = "payload.txt";
	public static boolean USE_CLIENT_AUTH = true;
	static String currentDirectory = System.getProperty("user.dir");
	public static String SERVER_CERT_PATH =  home + "/certs/serverCert/serverCert.pem";
	public static String CLIENT_PRIVKEY_PATH =  home + "/certs/clientCert/clientKey.pem";
	public static String CLIENT_CERT_PATH = home + "/certs/clientCert/clientCert.pem";
	public static String SERVER_PRIVKEY_PATH = home + "/certs/serverCert/serverKey.pem";
	public static String CA_CERT_PATH = home + "/certs/CACert.pem";
	public static final String TRUSTED_CA_PATH = home + "/certs/trustStore/";
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
