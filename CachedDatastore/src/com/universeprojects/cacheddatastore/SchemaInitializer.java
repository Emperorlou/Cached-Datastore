package com.universeprojects.cacheddatastore;

public class SchemaInitializer {
	static CachedSchema schema = null;
	
	public SchemaInitializer() {
		// TODO Auto-generated constructor stub
	}

	public static void initializeSchema(CachedSchema newSchema)
	{
		schema = newSchema;
	}
	
	public static CachedSchema getSchema()
	{
		return schema;
	}
}
