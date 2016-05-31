package com.universeprojects.cacheddatastore;

import java.util.List;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.Query.CompositeFilterOperator;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;

public class QueryHelper
{

	public QueryHelper()
	{
	}

	public static List<CachedEntity> fetchList(CachedDatastoreService ds, Cursor cursor, int limit, String kind)
	{
		return ds.fetchAsList(kind, null, limit, cursor);
	}
	
	public static List<CachedEntity> fetchList(CachedDatastoreService ds, Cursor cursor, int limit, String kind, String fieldName1, FilterOperator operator1, Object value1)
	{
		if (fieldName1==null)
			throw new IllegalArgumentException("Field name cannot be null.");
		if (operator1==null)
			throw new IllegalArgumentException("Operator cannot be null.");
		
		return ds.fetchAsList(kind, new FilterPredicate(fieldName1, operator1, value1), limit, cursor);
	}


	public static List<CachedEntity> fetchList(CachedDatastoreService ds, Cursor cursor, int limit, String kind, String fieldName1, FilterOperator operator1, Object value1, String fieldName2, FilterOperator operator2, Object value2)
	{
		if (fieldName1==null)
			throw new IllegalArgumentException("Field name cannot be null.");
		if (operator1==null)
			throw new IllegalArgumentException("Operator cannot be null.");
		if (fieldName2==null)
			throw new IllegalArgumentException("Field name cannot be null.");
		if (operator2==null)
			throw new IllegalArgumentException("Operator cannot be null.");
		
		Filter filter = CompositeFilterOperator.and(new FilterPredicate(fieldName1, operator1, value1), new FilterPredicate(fieldName2, operator2, value2));
		
		return ds.fetchAsList(kind, filter, limit, cursor);
	}


}
