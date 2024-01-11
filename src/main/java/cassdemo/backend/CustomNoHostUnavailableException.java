package cassdemo.backend;

import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class CustomNoHostUnavailableException extends Exception {
	private static final long serialVersionUID = 1L;

	public CustomNoHostUnavailableException(String message) {
		super(message);
	}

	public CustomNoHostUnavailableException(Exception e) {
		super(e);
	}

	public CustomNoHostUnavailableException(String message, Exception e) {
		super(message, e);
	}

	public CustomNoHostUnavailableException(String message, NoHostAvailableException e) {
		super(message, e);
	}
}