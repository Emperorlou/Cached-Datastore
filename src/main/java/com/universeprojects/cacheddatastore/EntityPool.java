package com.universeprojects.cacheddatastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	Map<Key, CachedEntity> pool = new HashMap<Key, CachedEntity>();
	
	public EntityPool(CachedDatastoreService ds)
	{
		this.ds = ds;
	}
	
	/**
	 * This loads the given keys and/or lists of keys into the pool. It does so in a single call.
	 * If one of the given keys is already in the pool, it will not be loaded again (that one will be skipped).
	 * 
	 * @param keylist This must be a series of either Iterable<Key> or Key type objects.
	 * @return The 'list' of entities that were added to the pool (doesn't include entities that were already in the pool).
	 */
	public Map<Key, CachedEntity> loadEntities(Object...keyList)
	{
		// Turn the given keyList mixed list into a list of keys we need to load (excluding keys that are already loaded into the pool)...
		List<Key> keysToLoad = new ArrayList<Key>();
		for(Object o:keyList)
		{
			if (o==null)
			{
				// Lets just skip this one
				continue;
			}
			else if (o instanceof Key)
			{
				if (pool.containsKey(o)==false)
					keysToLoad.add((Key)o);
			}
			else if (o instanceof Iterable)
			{
				@SuppressWarnings("unchecked")
				Iterable<Key> list = (Iterable<Key>)o;
				for(Key key:list)
				{
					if (key==null)
						continue;	// Skip this one
					else if ((key instanceof Key)==false)
						throw new IllegalArgumentException("One of the objects in a given Iterable was not Key type.");
					
					if (pool.containsKey(key)==false)
						keysToLoad.add(key);
				}
			}
			else
				throw new IllegalArgumentException("An unsupported type was given: "+o.getClass().getSimpleName()+". Supported classes are Key and Iterable.");
		}
		
		// Now load the list of entities we need
		Map<Key, CachedEntity> entities = ds.getAsMap(keysToLoad);
		
		// And add them into the pool
		pool.putAll(entities);
		
		return entities;
	}
	
	public CachedEntity get(Key entityKey)
	{
		if (pool.containsKey(entityKey)==false)
			throw new IllegalArgumentException("The entityKey was NOT preloaded into the EntityPool. All entities should be bulk loaded into a pool before they can be accessed.");
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
}
