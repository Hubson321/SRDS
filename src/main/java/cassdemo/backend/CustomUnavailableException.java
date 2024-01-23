package cassdemo.backend;

public class CustomUnavailableException extends Exception {
	private static final long serialVersionUID = 1L;

	public CustomUnavailableException(String message) {
		super(message);
	}

	public CustomUnavailableException(Exception e) {
		super(e);
	}

	public CustomUnavailableException(String message, Exception e) {
		super(message, e);
	}
}