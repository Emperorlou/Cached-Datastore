package com.universeprojects.cacheddatastore;

public class EntityAlreadyPutException extends RuntimeException
{
	private static final long serialVersionUID = 7775325671737359590L;

	public EntityAlreadyPutException(String message, Throwable e)
	{
		super(message, e);
	}
}
