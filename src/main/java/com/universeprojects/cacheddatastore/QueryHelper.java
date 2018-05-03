package com.universeprojects.cacheddatastore;

import java.util.List;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.CompositeFilter;
import com.google.appengine.api.datastore.Query.CompositeFilterOperator;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortDirection;

public class QueryHelper
{
	final private CachedDatastoreService ds;
	
	public QueryHelper(CachedDatastoreService ds)
	{
		this.ds = ds;
	}

	/**
	 * Use this if you only want to get a count of entities in the database.
	 * 
	 * This method will only count up to a maximum of 5000 entities.
	 * 
	 * This is more efficient than using the other filtered methods because it
	 * doesn't actually fetch all the data from the database, it only issues
	 * a special count query.
	 * 
	 * @param kind
	 * @param fieldName
	 * @param operator
	 * @param equalToValue
	 * @return
	 */
	public Long getFilteredList_Count(String kind, String fieldName, FilterOperator operator, Object equalToValue)
	{
		return getFilteredList_Count(kind, 5000, fieldName, operator, equalToValue);
	}
	/**
	 * Use this if you only want to get a count of entities in the database.
	 * 
	 * This method will only count up to the maximum amount specified in the limit parameter.
	 * 
	 * This is more efficient than using the other filtered methods because it
	 * doesn't actually fetch all the data from the database, it only issues
	 * a special count query.
	 * 
	 * @param kind
	 * @param limit
	 * @param fieldName
	 * @param operator
	 * @param equalToValue
	 * @return
	 */
	public Long getFilteredList_Count(String kind, Integer limit, String fieldName, FilterOperator operator, Object equalToValue)
	{
		Query q = new Query(kind);
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		q.setFilter(f1);
		return ds.countEntities(q, limit);
	}

	public Long getFilteredList_Count(String kind, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2)
	{
		return getFilteredList_Count(kind, 5000, fieldName, operator, equalToValue, fieldName2, operator2, equalToValue2);
	}
	
	public Long getFilteredList_Count(String kind, Integer limit, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2)
	{
		Query q = new Query(kind);
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, operator2, equalToValue2);
		Filter f = CompositeFilterOperator.and(f1, f2);
		q.setFilter(f);
		return ds.countEntities(q, limit);
	}

	public Long getFilteredList_Count(String kind, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2, String fieldName3, FilterOperator operator3, Object equalToValue3)
	{
		return getFilteredList_Count(kind, 5000, fieldName, operator, equalToValue, fieldName2, operator2, equalToValue2, fieldName3, operator3, equalToValue3);
	}
	
	public Long getFilteredList_Count(String kind, Integer limit, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2, String fieldName3, FilterOperator operator3, Object equalToValue3)
	{
		Query q = new Query(kind);
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, operator2, equalToValue2);
		FilterPredicate f3 = new FilterPredicate(fieldName3, operator3, equalToValue3);
		Filter f = CompositeFilterOperator.and(f1, f2, f3);
		q.setFilter(f);
		return ds.countEntities(q, 5000);
	}

	public List<CachedEntity> getFilteredList(String kind, int limit, Cursor cursor)
	{
		return ds.fetchAsList(kind, null, limit, cursor);
	}

	public List<CachedEntity> getFilteredList_Sorted(String kind, int limit, Cursor cursor, String fieldName, boolean ascending)
	{
		SortDirection direction = SortDirection.DESCENDING;
		if (ascending)
			direction = SortDirection.ASCENDING;
			
		Query q = new Query(kind).addSort(fieldName, direction);
		return ds.fetchAsList(q, limit, cursor);
	}

	public List<CachedEntity> getFilteredList(String kind, int limit, Cursor cursor, String fieldName, FilterOperator operator, Object equalToValue)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		return ds.fetchAsList(kind, f1, limit, cursor);
	}

	public List<CachedEntity> getFilteredList(String kind, int limit, Cursor cursor, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, operator2, equalToValue2);
		
		Filter filter = CompositeFilterOperator.and(f1, f2);
		return ds.fetchAsList(kind, filter, limit, cursor);
	}

	public List<CachedEntity> getFilteredList(String kind, String fieldName, Object equalToValue, String fieldName2, Object equalToValue2, String fieldName3, Object equalToValue3)
	{
		return getFilteredList(kind, 1000, null, fieldName, FilterOperator.EQUAL, equalToValue, fieldName2, FilterOperator.EQUAL, equalToValue2, fieldName3, FilterOperator.EQUAL, equalToValue3);
	}
	
	public List<CachedEntity> getFilteredList(String kind, int limit, Cursor cursor, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2, String fieldName3, FilterOperator operator3, Object equalToValue3)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, operator2, equalToValue2);
		FilterPredicate f3 = new FilterPredicate(fieldName3, operator3, equalToValue3);
		
		Filter filter = CompositeFilterOperator.and(f1, f2, f3);
		return ds.fetchAsList(kind, filter, limit, cursor);
	}

	public List<CachedEntity> getFilteredList(String kind, String fieldName, Object equalToValue, String fieldName2, Object equalToValue2, String fieldName3, Object equalToValue3, String fieldName4, Object equalToValue4)
	{
		return getFilteredList(kind, 1000, null, fieldName, FilterOperator.EQUAL, equalToValue, fieldName2, FilterOperator.EQUAL, equalToValue2, fieldName3, FilterOperator.EQUAL, equalToValue3, fieldName4, FilterOperator.EQUAL, equalToValue4);
	}
	
	public List<CachedEntity> getFilteredList(String kind, int limit, Cursor cursor, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2, String fieldName3, FilterOperator operator3, Object equalToValue3, String fieldName4, FilterOperator operator4, Object equalToValue4)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, operator2, equalToValue2);
		FilterPredicate f3 = new FilterPredicate(fieldName3, operator3, equalToValue3);
		FilterPredicate f4 = new FilterPredicate(fieldName4, operator4, equalToValue4);
		
		Filter filter = CompositeFilterOperator.and(f1, f2, f3, f4);
		return ds.fetchAsList(kind, filter, limit, cursor);
	}

	public List<CachedEntity> getFilteredList(String kind, int limit, String fieldName, FilterOperator operator, Object equalToValue)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		return ds.fetchAsList(kind, f1, limit);
	}

	public List<CachedEntity> getFilteredList(String kind, String fieldName, FilterOperator operator, Object equalToValue)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		return ds.fetchAsList(kind, f1, 1000);
	}


	/**
	 * This fetches all entities of a given kind, limited to the first 1000 results.
	 * 
	 * @param kind
	 * @return
	 */
	public List<CachedEntity> getFilteredList(String kind)
	{
		return getFilteredList(kind, 1000);
	}

	/**
	 * This fetches all entities of a given kind, but only up to the given limit.
	 * @param kind
	 * @param limit The maximum number of entities to return.
	 * @return
	 */
	public List<CachedEntity> getFilteredList(String kind, int limit)
	{
		return ds.fetchAsList(kind, null, limit);
	}

	public List<CachedEntity> getFilteredList(String kind, String fieldName, Object equalToValue)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, FilterOperator.EQUAL, equalToValue);
		return ds.fetchAsList(kind, f1, 1000);
	}

	public List<Key> getFilteredList_Keys(String kind, String fieldName, Object equalToValue)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, FilterOperator.EQUAL, equalToValue);
		return ds.fetchAsList_Keys(kind, f1, 1000);
	}

	public List<Key> getFilteredList_Keys(String kind, int limit, String fieldName, Object equalToValue)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, FilterOperator.EQUAL, equalToValue);
		return ds.fetchAsList_Keys(kind, f1, limit);
	}

	public List<Key> getFilteredList_Keys(String kind, int limit)
	{
		Query q = new Query(kind);
		return ds.fetchAsList_Keys(q, limit);
	}

	public List<Key> getFilteredList_Keys(String kind, int limit, Cursor cursor)
	{
		Query q = new Query(kind);
		return ds.fetchAsList_Keys(q, limit, cursor);
	}

	public List<Key> getFilteredList_Keys(String kind, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, operator2, equalToValue2);
		
		Filter filter = CompositeFilterOperator.and(f1, f2);
		return ds.fetchAsList_Keys(kind, filter, 1000);
	}

	public List<Key> getFilteredList_Keys(String kind, int limit, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, operator2, equalToValue2);
		
		Filter filter = CompositeFilterOperator.and(f1, f2);
		return ds.fetchAsList_Keys(kind, filter, 1000);
	}

	public List<Key> getFilteredList_Keys(String kind, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2, String fieldName3, FilterOperator operator3, Object equalToValue3)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, operator2, equalToValue2);
		FilterPredicate f3 = new FilterPredicate(fieldName3, operator3, equalToValue3);
		
		Filter filter = CompositeFilterOperator.and(f1, f2, f3);
		return ds.fetchAsList_Keys(kind, filter, 1000);
	}
	public List<CachedEntity> getFilteredList(String kind, String fieldName, Object equalToValue, String fieldName2, Object equalToValue2)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, FilterOperator.EQUAL, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, FilterOperator.EQUAL, equalToValue2);
		Filter filter = CompositeFilterOperator.and(f1, f2);
		return ds.fetchAsList(kind, filter, 1000);
	}

	public List<CachedEntity> getFilteredORList(Cursor cursor, String kind, String fieldName, Object equalToValue, String fieldName2, Object equalToValue2)
	{
//		FilterPredicate f1 = new FilterPredicate(fieldName, FilterOperator.EQUAL, equalToValue);
//		FilterPredicate f2 = new FilterPredicate(fieldName2, FilterOperator.EQUAL, equalToValue2);
//		Filter filter = CompositeFilterOperator.or(f1, f2);
//		Query q = new Query(kind);
//		q.setFilter(filter);
//
//		return ds.fetchAsList(q, 1000, cursor);
		List<CachedEntity> list = getFilteredList(kind, 1000, cursor, fieldName, FilterOperator.EQUAL, equalToValue);
		list.addAll(getFilteredList(kind, 1000, cursor, fieldName2, FilterOperator.EQUAL, equalToValue2));
		return list;
	}

	public List<CachedEntity> getFilteredORList(Cursor cursor, String kind, String fieldName, Object equalToValue, String fieldName2, Object equalToValue2, String fieldName3, Object equalToValue3)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, FilterOperator.EQUAL, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, FilterOperator.EQUAL, equalToValue2);
		FilterPredicate f3 = new FilterPredicate(fieldName3, FilterOperator.EQUAL, equalToValue3);
		Filter filter = CompositeFilterOperator.or(f1, f2, f3);
		Query q = new Query(kind);
		q.setFilter(filter);

		return ds.fetchAsList(q, 1000, cursor);
	}	
	
	/**
	 * This performs an ancestor query to fetch all children (within a limit) 
	 * @param ds
	 * @param parent
	 * @param limit
	 * @return
	 */
	public List<CachedEntity> fetchChildren(Key parent, int limit)
	{
		Query q = new Query();
		q.setAncestor(parent);
		return ds.fetchAsList(q, limit);
	}


}
