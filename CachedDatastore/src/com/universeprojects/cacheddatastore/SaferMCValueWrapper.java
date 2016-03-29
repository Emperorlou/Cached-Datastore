package com.universeprojects.cacheddatastore;

import java.io.Serializable;

public class SaferMCValueWrapper implements Serializable
{
	private static final long serialVersionUID = -136121424246543159L;
	Object value;
	public SaferMCValueWrapper(Object value)
	{
		this.value = value;
	}
}