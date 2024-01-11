package cassdemo.ObjectsModel;

public class CustomHostUnavailableException extends Exception {
	private static final long serialVersionUID = 1L;

	public CustomHostUnavailableException(String message) {
		super(message);
	}

	public CustomHostUnavailableException(Exception e) {
		super(e);
	}

	public CustomHostUnavailableException(String message, Exception e) {
		super(message, e);
	}
}