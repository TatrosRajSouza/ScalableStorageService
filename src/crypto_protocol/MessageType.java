package crypto_protocol;

/**
 * Contains all used message types
 * @author Elias Tatros
 *
 */
public enum MessageType {
	ClientInitMessage,
	ClientKeyExchangeMessage,
	CommandGET,
	ErrorMessage,
	ServerAuthConfirmationMessage,
	ServerInitMessage,
	EncryptedFileMessage;
}
