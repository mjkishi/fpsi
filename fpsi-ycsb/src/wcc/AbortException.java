package wcc;

public class AbortException extends Exception {
	private static final long serialVersionUID = 1L;

	public AbortException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public AbortException(String msg) {
		super(msg);
	}
}