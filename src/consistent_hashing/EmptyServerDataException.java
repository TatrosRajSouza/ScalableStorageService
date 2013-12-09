package consistent_hashing;

public class EmptyServerDataException extends Exception {
	private static final long serialVersionUID = 7901287446740487103L;

	public EmptyServerDataException() {}
	
	public EmptyServerDataException(String message) {
		super(message);
	}
}
