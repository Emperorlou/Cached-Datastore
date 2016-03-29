package com.universeprojects.cacheddatastore;

public class AbortTransactionException extends Exception {

	public AbortTransactionException(String message) {
		super(message);
	}

	public AbortTransactionException(Throwable cause) {
		super(cause);
	}

	public AbortTransactionException(String message, Throwable cause) {
		super(message, cause);
	}

	public AbortTransactionException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
