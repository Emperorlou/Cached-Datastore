package com.universeprojects.cacheddatastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.appengine.api.datastore.EmbeddedEntity;
import com.google.appengine.api.datastore.Key;

/**
 * This is used to efficiently load a tree of entities (or even just a list). It is meant 
 * as a temporary cache of CachedEntity loads and it functions primarily as a Key-CachedEntity store.
 * 
 * @author Owner
 *
 */
public class EntityPool
{
	final private CachedDatastoreService ds;
	Map<Object, CachedEntity> pool = new HashMap<>();
	
	Set<Key> queue = null;
	
	
	public EntityPool(CachedDatastoreService ds)
	{
		this.ds = ds;
	}
	
	/**
	 * Adds the given list of Key or List<Key> objects to an internal queue. 
	 * Calling loadEntities() (arguments or no arguments) will load the keys stored
	 * in this queue.
	 * 
	 * @param keyList
	 */
	public void addToQueue(Object...keyList)
	{
		if (keyList==null) return;
		
		List<Key> keysToLoad = new ArrayList<Key>();
		for(Object o:keyList)
		{
			if (o==null)
			{
				// Lets just skip this one
				continue;
			}
			else if (o instanceof CachedEntity)
			{
				addEntityDirectly((CachedEntity)o);
			}
			else if (o instanceof EmbeddedEntity)
			{
				addEmbeddedEntityDirectly((EmbeddedEntity)o);
			}
			else if (o instanceof Key)
			{
				if (pool.containsKey(o)==false && (queue==null || queue.contains(o)==false))
					keysToLoad.add((Key)o);
			}
			else if (o instanceof Iterable)
			{
				Iterable<?> list = (Iterable<?>)o;
				for(Object obj:list)
				{
					addToQueue(obj);
				}
			}
			else
				throw new IllegalArgumentException("An unsupported type was given: "+o.getClass().getSimpleName()+". Supported classes are Key and Iterable.");
		}
		
		if (keysToLoad.isEmpty()==false)
		{
			if (queue==null)
				queue = new HashSet<Key>();
			
			queue.addAll(keysToLoad);
		}
	}
	
	
	/**
	 * This loads the given keys and/or lists of keys into the pool. It does so in a single call.
	 * If one of the given keys is already in the pool, it will not be loaded again (that one will be skipped).
	 * 
	 * If any keys are waiting in the queue, they will be loaded as well. You can call loadEntities() (with no args)
	 * to simply load entities stored in the queue.
	 * 
	 * @param keylist This must be a series of either Iterable<Key> or Key type objects.
	 * @return The 'list' of entities that were added to the pool (doesn't include entities that were already in the pool).
	 */
	public Map<Key, CachedEntity> loadEntities(Object...keyList)
	{
		// Turn the given keyList mixed list into a list of keys we need to load (excluding keys that are already loaded into the pool)...
		List<Key> keysToLoad = new ArrayList<Key>();
		if (keyList!=null)
		{
			addToQueue(keyList);
		}
		
		
		if (queue!=null)
		{
			keysToLoad.addAll(queue);
			queue.clear();
		}
		
		if (keysToLoad.isEmpty()) return new HashMap<>();
		
		// Now load the list of entities we need
		Map<Key, CachedEntity> entities = ds.getAsMap(keysToLoad);
		
		// And add them into the pool
		pool.putAll(entities);
		
		return entities;
	}
	
	public CachedEntity get(Object entityKey)
	{
		if (entityKey==null) return null;
		if (pool.containsKey(entityKey)==false)
			throw new IllegalArgumentException("The entityKey '"+entityKey+"' was not preloaded into the EntityPool. All entities should be bulk loaded into a pool before they can be accessed.");
		return pool.get(entityKey);
	}
	
	public List<CachedEntity> get(List<Key> entityKeys)
	{
		if (entityKeys==null) return null;
		
		List<CachedEntity> result = new ArrayList<CachedEntity>();
		for(Key key:entityKeys)
			result.add(get(key));
		
		return result;
	}
	
	/**
	 * The number of entities that were loaded but came back null (entity not found). 
	 * 
	 * @return
	 */
	public int getFailedFetchCount()
	{
		int count = 0;
		for(CachedEntity e:pool.values())
			if (e==null)
				count++;
				
		return count;
	}
	
	/**
	 * Returns all of the keys that failed to load due to the entity not being found.
	 * 
	 * @return
	 */
	public List<Object> getFailedFetchKeys()
	{
		List<Object> result = new ArrayList<>();
		for(Object key:pool.keySet())
			if (pool.get(key)==null)
				result.add(key);
				
		return result;
	}

	public void addEmbeddedEntityDirectly(EmbeddedEntity...entity)
	{
		for(EmbeddedEntity e:entity)
		{
//			if (e.getKey()==null) throw new RuntimeException("Cannot add an embedded entity if it doesn't have a key.");
			
			CachedEntity wrapper = new CachedEntity(e.getKey());
			for(String fieldName:e.getProperties().keySet())
			{
				wrapper.setProperty(fieldName, e.getProperty(fieldName));
			}
			
			pool.put(e, wrapper);
		}
	}
	
	/**
	 * This allows you to add CachedEntity entities to the pool directly. Use this
	 * if you created a new entity and need it in the pool. Use this in any case where
	 * the entity is already in your possession and it doesn't need to be fetched from
	 * the DB.
	 * 
	 * @param entity
	 */
	public void addEntityDirectly(CachedEntity...entity)
	{
		for(CachedEntity e:entity)
		{
			Key key = e.getKey();
			if (pool.containsKey(key)==false)
				pool.put(key, e);
		}
		
	}

	public void addNullEntityDirectly(Key...keys)
	{
		for(Key key:keys)
		{
			pool.put(key, null);
		}
	}
	
	public boolean contains(Key key)
	{
		return pool.containsKey(key);
	}
}
