package common.messages;

public class InvalidMessageException extends Exception {

	private static final long serialVersionUID = 3208812901373211818L;

	public InvalidMessageException() {
		super();
	}
	
	public InvalidMessageException(String message) {
		super(message);
	}
}
