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
	 * @param kind The kind of entities to count.
	 * @param fieldName The field name to filter on.
	 * @param operator The filter operator.
	 * @param equalToValue The value to compare against.
	 * @return The count of entities that match the filter criteria.
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
	 * @param kind The kind of entities to count.
	 * @param limit The maximum number of entities to count.
	 * @param fieldName The field name to filter on.
	 * @param operator The filter operator.
	 * @param equalToValue The value to compare against.
	 * @return The count of entities that match the filter criteria.
	 */
	public Long getFilteredList_Count(String kind, Integer limit, String fieldName, FilterOperator operator, Object equalToValue)
	{
		Query q = new Query(kind);
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		q.setFilter(f1);
		return ds.countEntities(q, limit);
	}

	/**
	 * Use this if you only want to get a count of entities in the database with multiple filters.
	 * 
	 * This method will only count up to a maximum of 5000 entities.
	 * 
	 * This is more efficient than using the other filtered methods because it
	 * doesn't actually fetch all the data from the database, it only issues
	 * a special count query.
	 * 
	 * @param kind The kind of entities to count.
	 * @param fieldName The first field name to filter on.
	 * @param operator The first filter operator.
	 * @param equalToValue The first value to compare against.
	 * @param fieldName2 The second field name to filter on.
	 * @param operator2 The second filter operator.
	 * @param equalToValue2 The second value to compare against.
	 * @return The count of entities that match the filter criteria.
	 */
	public Long getFilteredList_Count(String kind, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2)
	{
		return getFilteredList_Count(kind, 5000, fieldName, operator, equalToValue, fieldName2, operator2, equalToValue2);
	}
	
	/**
	 * Use this if you only want to get a count of entities in the database with multiple filters.
	 * 
	 * This method will only count up to the maximum amount specified in the limit parameter.
	 * 
	 * This is more efficient than using the other filtered methods because it
	 * doesn't actually fetch all the data from the database, it only issues
	 * a special count query.
	 * 
	 * @param kind The kind of entities to count.
	 * @param limit The maximum number of entities to count.
	 * @param fieldName The first field name to filter on.
	 * @param operator The first filter operator.
	 * @param equalToValue The first value to compare against.
	 * @param fieldName2 The second field name to filter on.
	 * @param operator2 The second filter operator.
	 * @param equalToValue2 The second value to compare against.
	 * @return The count of entities that match the filter criteria.
	 */
	public Long getFilteredList_Count(String kind, Integer limit, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2)
	{
		Query q = new Query(kind);
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, operator2, equalToValue2);
		Filter f = CompositeFilterOperator.and(f1, f2);
		q.setFilter(f);
		return ds.countEntities(q, limit);
	}

	/**
	 * Use this if you only want to get a count of entities in the database with three filters.
	 * 
	 * This method will only count up to a maximum of 5000 entities.
	 * 
	 * This is more efficient than using the other filtered methods because it
	 * doesn't actually fetch all the data from the database, it only issues
	 * a special count query.
	 * 
	 * @param kind The kind of entities to count.
	 * @param fieldName The first field name to filter on.
	 * @param operator The first filter operator.
	 * @param equalToValue The first value to compare against.
	 * @param fieldName2 The second field name to filter on.
	 * @param operator2 The second filter operator.
	 * @param equalToValue2 The second value to compare against.
	 * @param fieldName3 The third field name to filter on.
	 * @param operator3 The third filter operator.
	 * @param equalToValue3 The third value to compare against.
	 * @return The count of entities that match the filter criteria.
	 */
	public Long getFilteredList_Count(String kind, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2, String fieldName3, FilterOperator operator3, Object equalToValue3)
	{
		return getFilteredList_Count(kind, 5000, fieldName, operator, equalToValue, fieldName2, operator2, equalToValue2, fieldName3, operator3, equalToValue3);
	}
	
	/**
	 * Use this if you only want to get a count of entities in the database with three filters.
	 * 
	 * This method will only count up to the maximum amount specified in the limit parameter.
	 * 
	 * This is more efficient than using the other filtered methods because it
	 * doesn't actually fetch all the data from the database, it only issues
	 * a special count query.
	 * 
	 * @param kind The kind of entities to count.
	 * @param limit The maximum number of entities to count.
	 * @param fieldName The first field name to filter on.
	 * @param operator The first filter operator.
	 * @param equalToValue The first value to compare against.
	 * @param fieldName2 The second field name to filter on.
	 * @param operator2 The second filter operator.
	 * @param equalToValue2 The second value to compare against.
	 * @param fieldName3 The third field name to filter on.
	 * @param operator3 The third filter operator.
	 * @param equalToValue3 The third value to compare against.
	 * @return The count of entities that match the filter criteria.
	 */
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

	/**
	 * Get a list of entities of a given kind, limited to a specified number of results.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param limit The maximum number of entities to return.
	 * @param cursor The cursor to start fetching from.
	 * @return The list of entities that match the filter criteria.
	 */
	public List<CachedEntity> getFilteredList(String kind, int limit, Cursor cursor)
	{
		return ds.fetchAsList(kind, null, limit, cursor);
	}

	/**
	 * Get a list of entities of a given kind, limited to a specified number of results and sorted by a field.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param limit The maximum number of entities to return.
	 * @param cursor The cursor to start fetching from.
	 * @param fieldName The field name to sort on.
	 * @param ascending True if the sorting should be in ascending order, false if descending.
	 * @return The list of entities that match the filter criteria and sorted by the specified field.
	 */
	public List<CachedEntity> getFilteredList_Sorted(String kind, int limit, Cursor cursor, String fieldName, boolean ascending)
	{
		SortDirection direction = SortDirection.DESCENDING;
		if (ascending)
			direction = SortDirection.ASCENDING;
			
		Query q = new Query(kind).addSort(fieldName, direction);
		return ds.fetchAsList(q, limit, cursor);
	}

	/**
	 * Get a list of entities of a given kind, limited to a specified number of results and filtered by a field.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param limit The maximum number of entities to return.
	 * @param cursor The cursor to start fetching from.
	 * @param fieldName The field name to filter on.
	 * @param operator The filter operator.
	 * @param equalToValue The value to compare against.
	 * @return The list of entities that match the filter criteria.
	 */
	public List<CachedEntity> getFilteredList(String kind, int limit, Cursor cursor, String fieldName, FilterOperator operator, Object equalToValue)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		return ds.fetchAsList(kind, f1, limit, cursor);
	}

	/**
	 * Get a list of entities of a given kind, limited to a specified number of results and filtered by two fields.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param limit The maximum number of entities to return.
	 * @param cursor The cursor to start fetching from.
	 * @param fieldName The first field name to filter on.
	 * @param operator The first filter operator.
	 * @param equalToValue The first value to compare against.
	 * @param fieldName2 The second field name to filter on.
	 * @param operator2 The second filter operator.
	 * @param equalToValue2 The second value to compare against.
	 * @return The list of entities that match the filter criteria.
	 */
	public List<CachedEntity> getFilteredList(String kind, int limit, Cursor cursor, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, operator2, equalToValue2);
		
		Filter filter = CompositeFilterOperator.and(f1, f2);
		return ds.fetchAsList(kind, filter, limit, cursor);
	}

	/**
	 * Get a list of entities of a given kind, filtered by three fields.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param fieldName The first field name to filter on.
	 * @param equalToValue The first value to compare against.
	 * @param fieldName2 The second field name to filter on.
	 * @param equalToValue2 The second value to compare against.
	 * @param fieldName3 The third field name to filter on.
	 * @param equalToValue3 The third value to compare against.
	 * @return The list of entities that match the filter criteria.
	 */
	public List<CachedEntity> getFilteredList(String kind, String fieldName, Object equalToValue, String fieldName2, Object equalToValue2, String fieldName3, Object equalToValue3)
	{
		return getFilteredList(kind, 1000, null, fieldName, FilterOperator.EQUAL, equalToValue, fieldName2, FilterOperator.EQUAL, equalToValue2, fieldName3, FilterOperator.EQUAL, equalToValue3);
	}
	
	/**
	 * Get a list of entities of a given kind, limited to a specified number of results and filtered by three fields.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param limit The maximum number of entities to return.
	 * @param cursor The cursor to start fetching from.
	 * @param fieldName The first field name to filter on.
	 * @param operator The first filter operator.
	 * @param equalToValue The first value to compare against.
	 * @param fieldName2 The second field name to filter on.
	 * @param operator2 The second filter operator.
	 * @param equalToValue2 The second value to compare against.
	 * @param fieldName3 The third field name to filter on.
	 * @param operator3 The third filter operator.
	 * @param equalToValue3 The third value to compare against.
	 * @return The list of entities that match the filter criteria.
	 */
	public List<CachedEntity> getFilteredList(String kind, int limit, Cursor cursor, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2, String fieldName3, FilterOperator operator3, Object equalToValue3)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, operator2, equalToValue2);
		FilterPredicate f3 = new FilterPredicate(fieldName3, operator3, equalToValue3);
		
		Filter filter = CompositeFilterOperator.and(f1, f2, f3);
		return ds.fetchAsList(kind, filter, limit, cursor);
	}

	/**
	 * Get a list of entities of a given kind, filtered by four fields.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param fieldName The first field name to filter on.
	 * @param equalToValue The first value to compare against.
	 * @param fieldName2 The second field name to filter on.
	 * @param equalToValue2 The second value to compare against.
	 * @param fieldName3 The third field name to filter on.
	 * @param equalToValue3 The third value to compare against.
	 * @param fieldName4 The fourth field name to filter on.
	 * @param equalToValue4 The fourth value to compare against.
	 * @return The list of entities that match the filter criteria.
	 */
	public List<CachedEntity> getFilteredList(String kind, String fieldName, Object equalToValue, String fieldName2, Object equalToValue2, String fieldName3, Object equalToValue3, String fieldName4, Object equalToValue4)
	{
		return getFilteredList(kind, 1000, null, fieldName, FilterOperator.EQUAL, equalToValue, fieldName2, FilterOperator.EQUAL, equalToValue2, fieldName3, FilterOperator.EQUAL, equalToValue3, fieldName4, FilterOperator.EQUAL, equalToValue4);
	}
	
	/**
	 * Get a list of entities of a given kind, limited to a specified number of results and filtered by four fields.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param limit The maximum number of entities to return.
	 * @param cursor The cursor to start fetching from.
	 * @param fieldName The first field name to filter on.
	 * @param operator The first filter operator.
	 * @param equalToValue The first value to compare against.
	 * @param fieldName2 The second field name to filter on.
	 * @param operator2 The second filter operator.
	 * @param equalToValue2 The second value to compare against.
	 * @param fieldName3 The third field name to filter on.
	 * @param operator3 The third filter operator.
	 * @param equalToValue3 The third value to compare against.
	 * @param fieldName4 The fourth field name to filter on.
	 * @param operator4 The fourth filter operator.
	 * @param equalToValue4 The fourth value to compare against.
	 * @return The list of entities that match the filter criteria.
	 */
	public List<CachedEntity> getFilteredList(String kind, int limit, Cursor cursor, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2, String fieldName3, FilterOperator operator3, Object equalToValue3, String fieldName4, FilterOperator operator4, Object equalToValue4)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, operator2, equalToValue2);
		FilterPredicate f3 = new FilterPredicate(fieldName3, operator3, equalToValue3);
		FilterPredicate f4 = new FilterPredicate(fieldName4, operator4, equalToValue4);
		
		Filter filter = CompositeFilterOperator.and(f1, f2, f3, f4);
		return ds.fetchAsList(kind, filter, limit, cursor);
	}

	/**
	 * Get a list of entities of a given kind, limited to a specified number of results and filtered by a field.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param limit The maximum number of entities to return.
	 * @param fieldName The field name to filter on.
	 * @param operator The filter operator.
	 * @param equalToValue The value to compare against.
	 * @return The list of entities that match the filter criteria.
	 */
	public List<CachedEntity> getFilteredList(String kind, int limit, String fieldName, FilterOperator operator, Object equalToValue)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		return ds.fetchAsList(kind, f1, limit);
	}

	/**
	 * Get a list of entities of a given kind, filtered by a field.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param fieldName The field name to filter on.
	 * @param operator The filter operator.
	 * @param equalToValue The value to compare against.
	 * @return The list of entities that match the filter criteria.
	 */
	public List<CachedEntity> getFilteredList(String kind, String fieldName, FilterOperator operator, Object equalToValue)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		return ds.fetchAsList(kind, f1, 1000);
	}


	/**
	 * This fetches all entities of a given kind, limited to the first 1000 results.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @return The list of entities that match the filter criteria.
	 */
	public List<CachedEntity> getFilteredList(String kind)
	{
		return getFilteredList(kind, 1000);
	}

	/**
	 * This fetches all entities of a given kind, but only up to the given limit.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param limit The maximum number of entities to return.
	 * @return The list of entities that match the filter criteria.
	 */
	public List<CachedEntity> getFilteredList(String kind, int limit)
	{
		return ds.fetchAsList(kind, null, limit);
	}

	/**
	 * Get a list of entities of a given kind, filtered by a field.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param fieldName The field name to filter on.
	 * @param equalToValue The value to compare against.
	 * @return The list of entities that match the filter criteria.
	 */
	public List<CachedEntity> getFilteredList(String kind, String fieldName, Object equalToValue)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, FilterOperator.EQUAL, equalToValue);
		return ds.fetchAsList(kind, f1, 1000);
	}

	/**
	 * Get a list of keys of entities of a given kind, filtered by a field.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param fieldName The field name to filter on.
	 * @param equalToValue The value to compare against.
	 * @return The list of keys of entities that match the filter criteria.
	 */
	public List<Key> getFilteredList_Keys(String kind, String fieldName, Object equalToValue)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, FilterOperator.EQUAL, equalToValue);
		return ds.fetchAsList_Keys(kind, f1, 1000);
	}

	/**
	 * Get a list of keys of entities of a given kind, limited to a specified number of results and filtered by a field.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param limit The maximum number of entities to return.
	 * @param fieldName The field name to filter on.
	 * @param equalToValue The value to compare against.
	 * @return The list of keys of entities that match the filter criteria.
	 */
	public List<Key> getFilteredList_Keys(String kind, int limit, String fieldName, Object equalToValue)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, FilterOperator.EQUAL, equalToValue);
		return ds.fetchAsList_Keys(kind, f1, limit);
	}

	/**
	 * Get a list of keys of entities of a given kind, limited to a specified number of results.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param limit The maximum number of entities to return.
	 * @return The list of keys of entities that match the filter criteria.
	 */
	public List<Key> getFilteredList_Keys(String kind, int limit)
	{
		Query q = new Query(kind);
		return ds.fetchAsList_Keys(q, limit);
	}

	/**
	 * Get a list of keys of entities of a given kind, limited to a specified number of results and starting from a cursor.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param limit The maximum number of entities to return.
	 * @param cursor The cursor to start fetching from.
	 * @return The list of keys of entities that match the filter criteria.
	 */
	public List<Key> getFilteredList_Keys(String kind, int limit, Cursor cursor)
	{
		Query q = new Query(kind);
		return ds.fetchAsList_Keys(q, limit, cursor);
	}

	/**
	 * Get a list of keys of entities of a given kind, filtered by two fields.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param fieldName The first field name to filter on.
	 * @param operator The first filter operator.
	 * @param equalToValue The first value to compare against.
	 * @param fieldName2 The second field name to filter on.
	 * @param operator2 The second filter operator.
	 * @param equalToValue2 The second value to compare against.
	 * @return The list of keys of entities that match the filter criteria.
	 */
	public List<Key> getFilteredList_Keys(String kind, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, operator2, equalToValue2);
		
		Filter filter = CompositeFilterOperator.and(f1, f2);
		return ds.fetchAsList_Keys(kind, filter, 1000);
	}

	/**
	 * Get a list of keys of entities of a given kind, limited to a specified number of results and filtered by two fields.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param limit The maximum number of entities to return.
	 * @param fieldName The first field name to filter on.
	 * @param operator The first filter operator.
	 * @param equalToValue The first value to compare against.
	 * @param fieldName2 The second field name to filter on.
	 * @param operator2 The second filter operator.
	 * @param equalToValue2 The second value to compare against.
	 * @return The list of keys of entities that match the filter criteria.
	 */
	public List<Key> getFilteredList_Keys(String kind, int limit, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, operator2, equalToValue2);
		
		Filter filter = CompositeFilterOperator.and(f1, f2);
		return ds.fetchAsList_Keys(kind, filter, 1000);
	}

	/**
	 * Get a list of keys of entities of a given kind, filtered by three fields.
	 * 
	 * @param kind The kind of entities to fetch.
	 * @param fieldName The first field name to filter on.
	 * @param operator The first filter operator.
	 * @param equalToValue The first value to compare against.
	 * @param fieldName2 The second field name to filter on.
	 * @param operator2 The second filter operator.
	 * @param equalToValue2 The second value to compare against.
	 * @param fieldName3 The third field name to filter on.
	 * @param operator3 The third filter operator.
	 * @param equalToValue3 The third value to compare against.
	 * @return The list of keys of entities that match the filter criteria.
	 */
	public List<Key> getFilteredList_Keys(String kind, String fieldName, FilterOperator operator, Object equalToValue, String fieldName2, FilterOperator operator2, Object equalToValue2, String fieldName3, FilterOperator operator3, Object equalToValue3)
	{
		FilterPredicate f1 = new FilterPredicate(fieldName, operator, equalToValue);
		FilterPredicate f2 = new FilterPredicate(fieldName2, operator2, equalToValue2);
		FilterPredicate f3 = new FilterPredicate(fieldName3, operator3, equalToValue3);
		
		Filter filter = CompositeFilterOperator.and(f1, f2, f3);
		return ds.fetchAsList_Keys(kind, filter, 1000);
	}

	/**
	 * Get a list of entities of a given kind, filtered by two fields using the OR operator.
	 * 
	 * @param cursor The cursor to start fetching from.
	 * @param kind The kind of entities to fetch.
	 * @param fieldName The first field name to filter on.
	 * @param equalToValue The first value to compare against.
	 * @param fieldName2 The second field name to filter on.
	 * @param equalToValue2 The second value to compare against.
	 * @return The list of entities that match the filter criteria.
	 */
	public List<CachedEntity> getFilteredORList(Cursor cursor, String kind, String fieldName, Object equalToValue, String fieldName2, Object equalToValue2)
	{
		List<CachedEntity> list = getFilteredList(kind, 1000, cursor, fieldName, FilterOperator.EQUAL, equalToValue);
		list.addAll(getFilteredList(kind, 1000, cursor, fieldName2, FilterOperator.EQUAL, equalToValue2));
		return list;
	}

	/**
	 * Get a list of entities of a given kind, filtered by three fields using the OR operator.
	 * 
	 * @param cursor The cursor to start fetching from.
	 * @param kind The kind of entities to fetch.
	 * @param fieldName The first field name to filter on.
	 * @param equalToValue The first value to compare against.
	 * @param fieldName2 The second field name to filter on.
	 * @param equalToValue2 The second value to compare against.
	 * @param fieldName3 The third field name to filter on.
	 * @param equalToValue3 The third value to compare against.
	 * @return The list of entities that match the filter criteria.
	 */
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
	 * Fetch all children entities of a given parent entity, limited to a specified number of results.
	 * 
	 * @param parent The parent entity key.
	 * @param limit The maximum number of entities to return.
	 * @return The list of children entities.
	 */
	public List<CachedEntity> fetchChildren(Key parent, int limit)
	{
		Query q = new Query();
		q.setAncestor(parent);
		return ds.fetchAsList(q, limit);
	}
}