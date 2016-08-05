package com.universeprojects.cacheddatastore;

import java.util.List;

public interface CachedSchema 
{
	boolean isFieldUnindexed(String entityKind, String fieldName);
	
	String getFieldTypeName(String entityKind, String fieldName);
	
	List<String> getKinds();
}
