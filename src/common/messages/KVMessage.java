package common.messages;

public interface KVMessage {
	public static final int BUFFER_SIZE = 1024;
	public static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
    public enum StatusType {
    	/* Flags as per specification */
    	GET,             		/* Get - request */
    	GET_ERROR,       		/* requested tuple (i.e. value) not found */
    	GET_SUCCESS,     		/* requested tuple (i.e. value) found */
    	PUT,               		/* Put - request */
    	PUT_SUCCESS,     		/* Put - request successful, tuple inserted */
    	PUT_UPDATE,      		/* Put - request successful, i.e., value updated */
    	PUT_ERROR,       		/* Put - request not successful */
    	DELETE_SUCCESS,  		/* Delete - request successful */
    	DELETE_ERROR,     		/* Delete - request successful */
    	SERVER_STOPPED,         /* Server is stopped, no requests are processed */
    	SERVER_WRITE_LOCK,      /* Server locked for out, only get possible */
    	SERVER_NOT_RESPONSIBLE, /* Request not successful, server not responsible for key */
    	/* Note: 3 Arguments Query. Send new Query with StatusType SERVER_NOT_RESPONSIBLE, 
    	 * set key to "metaData" (just the constant string) and 
    	 * set value to string representation of current meta data.
    	 * How to obtain the string representation: 
    	 * Use InfrastructureMetadata class to create new meta data and call toString().
    	 */
    	
    	/* Custom message flags */
		CONNECT,				/* Connect - request */
		CONNECT_SUCCESS,		/* Connect - request successful */
		CONNECT_ERROR,			/* Connect - request not successful */
		DISCONNECT,				/* Disconnect - request */
		DISCONNECT_SUCCESS,		/* Disconnect - request successful */
		FAILED 					/* Failed - unknown message or message too big*/
}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 * @throws InvalidMessageException if the message does not has a value
	 */
	public String getKey() throws InvalidMessageException;
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 * @throws InvalidMessageException if the message does not has a value
	 */
	public String getValue() throws InvalidMessageException;
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();
	
}


