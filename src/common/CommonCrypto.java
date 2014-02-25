package common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.SignatureException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.Mac;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.io.FileUtils;

/**
 * This class provides a set of common crypto functions 
 * @author Elias Tatros
 *
 */
public class CommonCrypto {
	
	/**
	 * Load trusted CAs from the trustStore
	 * @return A list of trusted X509 CA Certificates
	 * @throws CertificateException
	 * @throws Exception
	 */
	public static ArrayList<X509Certificate> loadTrustStore() throws CertificateException, Exception {
		ArrayList<X509Certificate> trustedCAList = new ArrayList<X509Certificate>();
		
		File baseDir = new File(Settings.TRUSTED_CA_PATH);
		Collection<File> trustedCAsList = FileUtils.listFiles(baseDir, null, true);
		
		for (File file : trustedCAsList) {
			trustedCAList.add(importCACertificate(file.getAbsolutePath()));
		}
		
		return trustedCAList;
	}
	
	
	/**
	 * Determine whether a given CA Certificate is trusted
	 * @param caCertificate
	 * @param trustedCAs
	 * @return true if the CA certificate is trusted, false otherwise
	 * @throws InvalidKeyException
	 * @throws CertificateException
	 * @throws NoSuchAlgorithmException
	 * @throws NoSuchProviderException
	 * @throws SignatureException
	 */
	public static boolean isCATrusted(X509Certificate caCertificate, ArrayList<X509Certificate> trustedCAs) 
			throws InvalidKeyException, CertificateException, NoSuchAlgorithmException, NoSuchProviderException, SignatureException {
		for (X509Certificate trustedCA : trustedCAs) {
			try {
			verifyCertificate(caCertificate, trustedCA);
			return true;
			} catch (CertificateException e) {
			}
		}
		
		return false;
	}
	
	
	/**
	 * Encrypt plain source using given algorithm and key
	 * @param plainSource data to encrypt
	 * @param algorithm algorithm that should be used
	 * @param key key for encryption
	 * @return encrypted data
	 * @throws NoSuchAlgorithmException
	 * @throws NoSuchPaddingException
	 * @throws InvalidKeyException
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 */
	public static byte[] encrypt (byte[] plainSource, String algorithm, Key key) throws NoSuchAlgorithmException,
			NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
		Cipher rsaCipher = Cipher.getInstance(algorithm);
		rsaCipher.init(Cipher.ENCRYPT_MODE, key);
		return rsaCipher.doFinal(plainSource);
	}
	
	
	/**
	 * Decrypt using AES
	 * @param encryptedSource
	 * @param algorithm
	 * @param key
	 * @param IV
	 * @return
	 */
	public static byte[] decryptAES(byte[] encryptedSource, String algorithm, Key key, byte[] IV) {
		
		try {
			SecretKeySpec keySpec = new SecretKeySpec(key.getEncoded(), "AES");
			Cipher cipher = Cipher.getInstance(algorithm);
			
			IvParameterSpec IVSpec = new IvParameterSpec(IV);
			cipher.init(Cipher.DECRYPT_MODE, keySpec, IVSpec);
			System.out.println("Cipher init");
			return cipher.doFinal(encryptedSource);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalArgumentException("Unable to decrypt contents, using " + algorithm + ", Message: " + e.getMessage());
		} catch (NoSuchPaddingException e) {
			throw new IllegalArgumentException("Unable to decrypt contents, using " + algorithm + ", Message: " + e.getMessage());
		} catch (InvalidKeyException e) {
			throw new IllegalArgumentException("Unable to decrypt contents, using " + algorithm + ", Message: " + e.getMessage());
		} catch (IllegalBlockSizeException e) {
			throw new IllegalArgumentException("Unable to decrypt contents, using " + algorithm + ", Message: " + e.getMessage());
		} catch (BadPaddingException e) {
			throw new IllegalArgumentException("Unable to decrypt contents, using " + algorithm + ", Message: " + e.getMessage());
		} catch (InvalidAlgorithmParameterException e) {
			throw new IllegalArgumentException("Unable to decrypt contents, using " + algorithm + ", Message: " + e.getMessage());
		}
		
	}
	
	
	/**
	 * Decrypt using RSA
	 * @param encryptedSource
	 * @param algorithm
	 * @param key
	 * @return
	 */
	public static byte[] decryptRSA (byte[] encryptedSource, String algorithm, Key key) {
		try {
			//Try to Decrypt  
			Cipher cipher = Cipher.getInstance(algorithm);
			cipher.init(Cipher.DECRYPT_MODE, key);
			return cipher.doFinal(encryptedSource);
		} catch (IllegalBlockSizeException e) {
			throw new IllegalArgumentException("Unable to decrypt using " + algorithm + ", invalid block size. Message: " + e.getMessage());
		} catch (BadPaddingException e) {
			throw new IllegalArgumentException("Unable to decrypt using " + algorithm + ", bad padding. Message: " + e.getMessage());
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalArgumentException("Unable to initialize " + algorithm + " for decryption, invalid algorithm. Message: " + e.getMessage());
		} catch (NoSuchPaddingException e) {
			throw new IllegalArgumentException("Unable to initialize " + algorithm + " for decryption, invalid padding. Message: " + e.getMessage());
		} catch (InvalidKeyException e) {
			throw new IllegalArgumentException("Unable to initialize " + algorithm + " for decryption, invalid key. Message: " + e.getMessage());
		}
	}
	
	
	/**
	 * Sign message using private key.
	 * @param message
	 * @param algorithm
	 * @param key
	 * @return
	 */
	public static byte[] sign (byte[] message, String algorithm, PrivateKey key) {
		// Try to Sign  
		try {
			Cipher cipher = Cipher.getInstance(algorithm);
			cipher.init(Cipher.ENCRYPT_MODE, key);
			return cipher.doFinal(message);
		} catch (IllegalBlockSizeException e) {
			throw new IllegalArgumentException("Unable to sign using " + algorithm + ", invalid block size. Message: " + e.getMessage());
		} catch (BadPaddingException e) {
			throw new IllegalArgumentException("Unable to sign using " + algorithm + ", bad padding. Message: " + e.getMessage());
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalArgumentException("Unable to initialize " + algorithm + " for encryption, invalid algorithm. Message: " + e.getMessage());
		} catch (NoSuchPaddingException e) {
			throw new IllegalArgumentException("Unable to initialize " + algorithm + " for encryption, invalid padding. Message: " + e.getMessage());
		} catch (InvalidKeyException e) {
			throw new IllegalArgumentException("Unable to initialize " + algorithm + " for encryption, invalid key. Message: " + e.getMessage());
		}
	}
	
	
	/**
	 * Load a pkcs 8 private key from file
	 * @param privKeyPath path to the pkcs8 file
	 * @param encoding
	 * @return PrivateKey the private key.
	 * @throws IOException
	 * @throws InvalidKeySpecException
	 * @throws NoSuchAlgorithmException
	 */
	public static PrivateKey loadPrivateKey(String privKeyPath, Charset encoding) throws IOException, InvalidKeySpecException, NoSuchAlgorithmException {
		RandomAccessFile raf = new RandomAccessFile(privKeyPath, "r");
		byte[] buf = new byte[(int)raf.length()];
		raf.readFully(buf);
		raf.close();
		PKCS8EncodedKeySpec kspec = new PKCS8EncodedKeySpec(buf);
		KeyFactory kf = KeyFactory.getInstance("RSA");
		return kf.generatePrivate(kspec);
	}
	
	
	/**
	 * Concatenate two byte arrays
	 * @param first
	 * @param second
	 * @return byte[] the concatenated array
	 * @throws IOException
	 */
	public static byte[] concatenateByteArray(byte[] first, byte[] second) throws IOException {
		ByteArrayOutputStream contentStream = new ByteArrayOutputStream();
		contentStream.write(first);
		contentStream.write(second);
		return contentStream.toByteArray();
	}
	
	
	/**
	 * Generate a custom session key
	 * @param algorithm
	 * @param masterSecret
	 * @param clientNonce
	 * @param serverNonce
	 * @param salt
	 * @return SecretKeySpec the generated key
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws IOException
	 */
	public static SecretKeySpec generateSessionKey(String algorithm, byte[] masterSecret, byte[] clientNonce, byte[] serverNonce, byte[] salt) throws NoSuchAlgorithmException, InvalidKeyException, IOException {
		Mac hmac = Mac.getInstance("HmacSHA1");
		
		SecretKeySpec macKey = new SecretKeySpec(masterSecret, algorithm);
		
		hmac.init(macKey);
		// System.out.println("HMAC LENGTH ---- : " + hmac.getMacLength());
		// System.out.println("MACKEY LENGTH ---- : " + macKey.getEncoded().length);
		
		ByteArrayOutputStream contentStream = new ByteArrayOutputStream();
		contentStream.write(clientNonce);
		contentStream.write(serverNonce);
		contentStream.write(salt);
		byte[] sessionKey = Arrays.copyOfRange(hmac.doFinal(contentStream.toByteArray()), 0, 16);
		return new SecretKeySpec(sessionKey, algorithm);
	}
	
	
	/**
	 * Generate a session hash using HMAC with given key
	 * @param algorithm
	 * @param macKey
	 * @param clientNonce
	 * @param serverNonce
	 * @param serverCertificate
	 * @param clientAuth
	 * @return byte[] the message hash
	 * @throws NoSuchAlgorithmException
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 * @throws InvalidKeyException
	 */
	public static byte[] generateSessionHash(String algorithm, SecretKeySpec macKey, 
			byte[] clientNonce, byte[] serverNonce, byte[] serverCertificate, boolean clientAuth) 
			throws NoSuchAlgorithmException, UnsupportedEncodingException, IOException, InvalidKeyException {
		
		Mac hmac = Mac.getInstance(algorithm);
		
		ByteArrayOutputStream contentStream = new ByteArrayOutputStream();
		contentStream.write("ClientInit".getBytes(Settings.CHARSET));
		contentStream.write(clientNonce);
		contentStream.write(Settings.TRANSFER_ENCRYPTION.getBytes(Settings.CHARSET));
		contentStream.write("ServerInit".getBytes(Settings.CHARSET));
		contentStream.write(serverNonce);
		contentStream.write(serverCertificate);
		
		if (clientAuth)
			contentStream.write("CertReq".getBytes(Settings.CHARSET));
		
		hmac.init(macKey);
		return hmac.doFinal(contentStream.toByteArray());
	}
	
	
	/**
	 * Generate hash of byte data using HMAC with given key
	 * @param algorithm
	 * @param macKey
	 * @param data
	 * @return byte[] hash of the data
	 * @throws NoSuchAlgorithmException
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 * @throws InvalidKeyException
	 */
	public static byte[] generateHash(String algorithm, SecretKeySpec macKey, 
			byte[] data) 
			throws NoSuchAlgorithmException, UnsupportedEncodingException, IOException, InvalidKeyException {
		
		Mac hmac = Mac.getInstance(algorithm);
		hmac.init(macKey);
		return hmac.doFinal(data);
	}
	
	
	/**
	 * Generate the confirmation hash using HMAC with given key (simple authentication)
	 * @param algorithm
	 * @param macKey
	 * @param encryptedSecret
	 * @param secureSessionHash
	 * @return byte[] the confirmation hash
	 * @throws NoSuchAlgorithmException
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 * @throws InvalidKeyException
	 */
	public static byte[] generateConfirmationHash(String algorithm, SecretKeySpec macKey, 
			byte[] encryptedSecret, byte[] secureSessionHash) 
			throws NoSuchAlgorithmException, UnsupportedEncodingException, IOException, InvalidKeyException {
		
		Mac hmac = Mac.getInstance(algorithm);
		
		ByteArrayOutputStream contentStream = new ByteArrayOutputStream();
		contentStream.write("ClientKex".getBytes(Settings.CHARSET));
		contentStream.write(encryptedSecret);
		contentStream.write(secureSessionHash);
		
		hmac.init(macKey);
		return hmac.doFinal(contentStream.toByteArray());
	}
	
	
	/**
	 * Generate the confirmation hash using HMAC with given key (mutual authentication)
	 * @param algorithm
	 * @param macKey
	 * @param encryptedSecret
	 * @param secureSessionHash
	 * @param clientCertificate
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 * @throws InvalidKeyException
	 */
	public static byte[] generateConfirmationHash(String algorithm, SecretKeySpec macKey, 
			byte[] encryptedSecret, byte[] secureSessionHash, X509Certificate clientCertificate) 
			throws NoSuchAlgorithmException, UnsupportedEncodingException, IOException, InvalidKeyException {
		
		Mac hmac = Mac.getInstance(algorithm);
		
		ByteArrayOutputStream contentStream = new ByteArrayOutputStream();
		contentStream.write("ClientKex".getBytes(Settings.CHARSET));
		contentStream.write(encryptedSecret);
		contentStream.write(secureSessionHash);
		
		try {
			contentStream.write(clientCertificate.getEncoded());
		} catch (CertificateEncodingException e) {
			throw new IllegalArgumentException("Unable to generate auth confirmation Hash. Encoding of client Certificate is invalid.\nMessage: " + e.getMessage());
		}
		
		hmac.init(macKey);
		return hmac.doFinal(contentStream.toByteArray());
	}
	
	
	/**
	 * Import a CA certificate from file
	 * @param path path to the certificate file
	 * @return X509Certificate the CA certificate
	 * @throws CertificateException
	 */
	public static X509Certificate importCACertificate(String path) throws CertificateException {
		try {
			X509Certificate caCertificate;
			File certificateFile = new File(path);
			CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
			caCertificate = (X509Certificate) certFactory.generateCertificate(new FileInputStream(certificateFile));
			
			/* Verify self-signed CA Certificate */
			caCertificate.verify(caCertificate.getPublicKey());
			return caCertificate;
		} catch (CertificateException e) {
			throw new CertificateException("Unable to create X.509 CA Certificate from file: " + path);
		} catch (FileNotFoundException e) {
			throw new CertificateException("X.509 CA Certificate file not found at: " + path);
		} catch (InvalidKeyException e) {
			throw new CertificateException("Invalid key for X.509 CA Certificate file: " + path + 
					"\nMessage:" + e.getMessage());
		} catch (NoSuchAlgorithmException e) {
			throw new CertificateException("Invalid cipher for X.509 CA Certificate file: " + path + 
					"\nMessage:" + e.getMessage());
		} catch (NoSuchProviderException e) {
			throw new CertificateException("Invalid provider for X.509 CA Certificate file: " + path + 
					"\nMessage:" + e.getMessage());
		} catch (SignatureException e) {
			throw new CertificateException("Invalid signature for X.509 CA Certificate file: " + path + 
					"\nMessage:" + e.getMessage());
		}
	}
	
	
	/**
	 * Verify that given X509Certificate is valid and issued/signed by specified CA
	 * @param certificate certificate to validate
	 * @param caCertificate certificate of issuer CA
	 * @throws InvalidKeyException
	 * @throws CertificateException
	 * @throws NoSuchAlgorithmException
	 * @throws NoSuchProviderException
	 * @throws SignatureException
	 */
	public static void verifyCertificate(X509Certificate certificate, X509Certificate caCertificate) throws InvalidKeyException, CertificateException, NoSuchAlgorithmException, NoSuchProviderException, SignatureException {
		
		if (certificate == null)
			throw new CertificateException("The specified subject certificate was null.");
		if (caCertificate == null)
			throw new CertificateException("The specified CA certificate was null.");
		
		/*
		 * Verify that issuer of certificate is subject of CA certificate 
		 */
		if (certificate.getIssuerX500Principal().getName().equals(caCertificate.getSubjectX500Principal().getName())) {
			/*
			 * Verify the Certificate is not expired
			 */
			certificate.checkValidity();
			
			/* 
			 * Verify that the signature of the certificate 
			 * was generated with the private key of CA (minissl-ca)
			 */
			certificate.verify(caCertificate.getPublicKey());
		} else {
			throw new CertificateException("miniSSL CA (" + caCertificate.getSubjectX500Principal().getName() + 
					") not issuer of certificate (" + certificate.getIssuerX500Principal().getName() + ")");
		}
		
	}
	

	/**
	 * Compare two byte arrays
	 * @param arr1
	 * @param arr2
	 * @return true if equal, false otherwise
	 */
	public static boolean isByteArrayEqual(byte[] arr1, byte[] arr2) {
		if (arr1.length != arr2.length)
			return false;
		
		for (int i = 0; i < arr1.length; i++) {
			if (arr1[i] != arr2[i]) {
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Convert an Object to byte Array
	 * @param obj The object that should be converted to bytes.
	 * @return Object as byte array
	 * @throws IOException
	 */
	public static byte[] objectToByteArray(Object obj) throws IOException {
		byte[] objectBytes = null;
		ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
		ObjectOutput out = null;
		
		
		try {
		  out = new ObjectOutputStream(byteArrayOutput);   
		  out.writeObject(obj);
		  objectBytes = byteArrayOutput.toByteArray();
		} finally {
		  try {
		    if (out != null) {
		      out.close();
		    }
		  } catch (IOException ex) {
		    System.out.println("Object->ByteArray - IO EX while closing ObjectOutputStream:\n" + ex.getMessage());
		  }
		  
		  try {
			  byteArrayOutput.close();
		  } catch (IOException ex) {
			  System.out.println("Object->ByteArray - IO EX while closing ByteArrayOutputStream:\n" + ex.getMessage());
		  }
		}
		
		return objectBytes;
	}
	
	/**
	 * Convert byte array into Object
	 * @param bytes The byte array that should be converted into an object.
	 * @return byte array as Object
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Object objectFromByteArray(byte[] bytes) throws IOException, ClassNotFoundException {
		Object o = null;
		ByteArrayInputStream byteArrayInput = new ByteArrayInputStream(bytes);
		ObjectInput in = null;
		try {
		  in = new ObjectInputStream(byteArrayInput);
		  o = in.readObject(); 
		} finally {
			try {
				byteArrayInput.close();
			} catch (IOException ex) {
				System.out.println("ByteArray->Object - IO EX while closing ByteArrayInputStream:\n" + ex.getMessage());
			}
			
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				System.out.println("ByteArray->Object - IO EX while closing ObjectInputStream:\n" + ex.getMessage());
			}
		}
		
		return o;
	}
}
