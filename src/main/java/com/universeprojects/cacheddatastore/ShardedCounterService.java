package com.universeprojects.cacheddatastore;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import com.google.appengine.api.datastore.EmbeddedEntity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.CompositeFilterOperator;
import com.google.appengine.api.datastore.Query.Filter;

public class ShardedCounterService
{
	private CachedDatastoreService ds = null;
	public static ShardedCounterService instance = null;
	public static final String SHARD_COUNTER_KIND = "_ShardCounter";


	private final Random randomGenerator = new Random();
	private static final Logger log = Logger.getLogger(ShardedCounterService.class.toString());

	/**
	 * Returns a singleton of the ShardedCounterService
	 * 
	 * @return
	 */
	public static ShardedCounterService getInstance(CachedDatastoreService ds)
	{
		if (ds == null) throw new IllegalArgumentException("CachedDatastoreService cannot be null.");

		if (instance != null) return instance;

		instance = new ShardedCounterService();
		instance.ds = ds;
		return instance;
	}

	
//	/**
//	 * This initializes a new sharded counter at the specified entity and fieldName. This should only be called once
//	 * per field sharded counter field.
//	 *  
//	 * @param entity
//	 * @param fieldName
//	 * @param numShards
//	 * @return
//	 */
//	private EmbeddedEntity initializeCounter(CachedEntity entity, String fieldName, Integer numShards)
//	{
//		if (numShards==null) numShards = 5;
//		
//		if (numShards < 1) 
//			throw new IllegalArgumentException("The number of shards for a sharded counter must be greater than 0.");
//		EmbeddedEntity zeroShard = (EmbeddedEntity) entity.getProperty(fieldName);
//
//		int shards = 0;
//		int num = numShards;
//		if (zeroShard != null)
//		{
//			shards = getNumShards(zeroShard);
//		}
//		else
//		{
//			zeroShard = new EmbeddedEntity();
//			entity.setUnindexedPropertyManually(fieldName, zeroShard);
//			shards = 0;
//		}
//		final int numIncr = num;
//		shards += numIncr;
//		zeroShard.setUnindexedProperty("shards", shards);
//		
//		ds.put(entity);
//		
//		return zeroShard;
//	}

	
	/**
	 * Increments the given counter (by the entity's key and fieldName) by 1.
	 * 
	 * @param entity
	 * @param key
	 * @return
	 */
	public void incrementCounter(Key entityKey, String fieldName, int defaultShardCount)
	{
		incrementCounter(entityKey, fieldName, 1L, defaultShardCount);
	}

	
	/**
	 * Increments the given counter (found by the entity's key and fieldName) by the given value.
	 * 
	 * NOTE: If this is the first time the sharded counter has been used, the entity parameter will be
	 * put to the database.
	 * 
	 * @param entity
	 * @param fieldName
	 * @param increment
	 * @return
	 */
	public void incrementCounter(Key entityKey, String fieldName, long increment, Integer defaultShardCount)
	{
		if (defaultShardCount == null)
			defaultShardCount = 5;
//		EmbeddedEntity zeroShard = (EmbeddedEntity) entity.getProperty(fieldName);
//		if (zeroShard == null) 
//			 zeroShard = initializeCounter(entity, fieldName, defaultShardCount);
		
		Integer numShards = defaultShardCount;
		while(true)
		{
		
			ds.beginTransaction();
	
	
			int rndNum = randomGenerator.nextInt(numShards) + 1;
	 		String shardName = getShardName(entityKey, fieldName, rndNum);
	
			Key shardKey = KeyFactory.createKey(SHARD_COUNTER_KIND, shardName);
			CachedEntity shard = ds.getIfExists(shardKey);
	
			if (shard != null)
			{
				long val = (Long) shard.getProperty("value");
				shard.setUnindexedPropertyManually("value", val + increment);
			}
			else
			{
				shard = new CachedEntity(shardKey);
				shard.setUnindexedPropertyManually("value", increment);
				shard.setProperty("entity", entityKey);
				shard.setProperty("fieldName", fieldName);
			}
			ds.put(shard);
	
			try
			{
				ds.commit();
			}
			catch (ConcurrentModificationException ignored)
			{
				ds.rollbackIfActive();
				continue;	// TRY AGAIN
			}
			return;
		}
	}

	/**
	 * Retrieves the value of the counter identified by the entity and fieldName. 
	 * 
	 * It attempts to return the memcache cache value. If that is null then the shards are 
	 * queried for and counted instead. 
	 * 
	 * This method will return null if a sharded counter has not yet been initialized for the given entity-field.
	 * 
	 * @param entity
	 * @param fieldName
	 * @return
	 */
	public Long readCounter(Key entityKey, String fieldName)
	{
		long value = 0;

		Query query = new Query(SHARD_COUNTER_KIND);
		query.setFilter(getShardQueryFilterFor(entityKey, fieldName));
		for (CachedEntity shard : ds.fetchAsList(query, 1000))
			value += (Long) shard.getProperty("value");

		return value;
	}

	/**
	 * This method is used to reset the counter back to 0. It deletes all shards.
	 * 
	 * @param entity
	 * @param key
	 */
	public void resetCounter(Key entityKey, String fieldName)
	{
		Query query = new Query(SHARD_COUNTER_KIND);
		query.setFilter(getShardQueryFilterFor(entityKey, fieldName));
		query.setKeysOnly();
		List<Key> keys = new ArrayList<>();
		for (CachedEntity en : ds.fetchAsIterable(query))
		{
			keys.add(en.getKey());
		}
		ds.delete(keys);
	}
	
	/**
	 * This method returns the entity's sharded counter field to the state it was in
	 * before initializeCounter() was called. It also deletes all shards associated
	 * with this counter from the database with a call to resetCounter().
	 * 
	 * NOTE: The entity is put to the database in this method.
	 * 
	 * @param entity
	 * @param fieldName
	 */
	public void nullifyCounter(Key entityKey, String fieldName)
	{
		resetCounter(entityKey, fieldName);
	}


	private static String getShardName(Key entityKey, String fieldName, int num)
	{
		String name = entityKey.toString()+fieldName;
		if (num > 0) name += "." + num;
		return name;
	}

//	/**
//	 * Determines the shard count specified in the zeroShard entity.
//	 * 
//	 * @param zeroShard
//	 * @return
//	 */
//	private int getNumShards(EmbeddedEntity zeroShard)
//	{
//		return ((Number) zeroShard.getProperty("shards")).intValue();
//	}
	
	/**
	 * This returns a datastore query filter that would fetch all of the
	 * shards associated with a given entity and field. 
	 * 
	 * @param entity
	 * @param fieldName
	 * @return
	 */
	private Filter getShardQueryFilterFor(Key entityKey, String fieldName)
	{
		return CompositeFilterOperator.and(
				new Query.FilterPredicate("entity", Query.FilterOperator.EQUAL, entityKey), 
				new Query.FilterPredicate("fieldName", Query.FilterOperator.EQUAL, fieldName));
	}

}
