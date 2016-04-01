package com.universeprojects.cacheddatastore;

import java.util.List;

public interface CachedSchema 
{
	public boolean isFieldUnindexed(String entityKind, String fieldName);
	
	public List<String> getKinds();
}
