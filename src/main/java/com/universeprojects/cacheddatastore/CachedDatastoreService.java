package com.universeprojects.cacheddatastore;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyRange;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.TransactionOptions;
import com.google.appengine.api.memcache.Expiration;
import com.google.appengine.api.memcache.InvalidValueException;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheService.IdentifiableValue;
import com.google.appengine.api.memcache.MemcacheService.SetPolicy;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.tools.remoteapi.RemoteApiInstaller;
import com.google.appengine.tools.remoteapi.RemoteApiOptions;


public class CachedDatastoreService
{
	final public static boolean statsTracking = false;
	final public static String MC_GETS = "Stats_MC_GETS";
	final public static String DS_GETS = "Stats_DS_GETS";
	final public static String QUERIES = "Stats_QUERIES";
	final public static String QUERY_ENTITIES = "Stats_QUERY_ENTITIES";
	final public static String MC_QUERY_ENTITIES = "Stats_MC_QUERY_ENTITIES";
	final public static String MC_QUERIES = "Stats_MC_QUERIES";

	final public static String QUERYKEYCACHE_QUERIES = "Stats_MC_QUERYKEYCACHE_QUERIES";
	final public static String QUERYKEYCACHE_MC_ENTITIES = "Stats_MC_QUERYKEYCACHE_MC_ENTITIES";
	final public static String QUERYKEYCACHE_DB_ENTITIES = "Stats_MC_QUERYKEYCACHE_DB_ENTITIES";
	
	final public String mcPrefix = "MCENTITY"; 
	boolean cacheEnabled = true;
	boolean queryModelCacheEnabled = false;
	
	DatastoreService db = null;
	MemcacheService mc = null;
	
	boolean enforceEntityFetchWithinTransaction = false;
	Set<Key> transactionallyFetchedEntities = null;
	Set<Key> transactionallyDeletedEntities = null;
	Map<Key, Entity> transactionallyChangedEntities = null;
	
	PreparedQuery pq = null;
	Cursor lastQuery_endCursor = null;
	
	

	
	
	public class EntityNotFetchedWithinTransactionException extends RuntimeException
	{
		private static final long serialVersionUID = 6909925746832395039L;
		private Entity entity;
		public EntityNotFetchedWithinTransactionException(Entity entity)
		{
			super("The entity "+entity.getKey().getKind()+"("+entity.getKey().getId()+") was not fetched within the active transaction.");
			this.entity = entity;
		}
		
		public Entity getEntity()
		{
			return entity;
		}
	}
	
	
	

	
	private static RemoteApiOptions options = null;
	private static boolean disableRemoteAPI = false;

	public static void disableRemoteAPI() {
		disableRemoteAPI = true;
	}
	
	public CachedDatastoreService()
	{
		if (disableRemoteAPI==false && SystemProperty.environment.value() != SystemProperty.Environment.Value.Production)
		{
			
			if (options==null)
				//noinspection deprecation
				//options = new RemoteApiOptions().server("playinitium.appspot.com", 443).credentials(System.getProperty("email"), System.getProperty("password"));
				options = new RemoteApiOptions().server("playinitium.appspot.com", 443).useApplicationDefaultCredential();			
			try
			{
				RemoteApiInstaller installer = new RemoteApiInstaller();
				installer.install(options);
			}
			catch(IllegalStateException ise)
			{
				// Ignore. Remote API is probably already installed
			}
			catch(Exception e)
			{
				// Ok fine, no remote API
				disableRemoteAPI=true;
				Logger.getLogger(CachedDatastoreService.class.toString()).log(Level.WARNING, "Failed to connect to remote API", e);
			}
		}
		
		db = DatastoreServiceFactory.getDatastoreService();
		mc = getMC();
	}
	
	public MemcacheService getMC()
	{
		if (mc!=null)
			return mc;
		
		if (disableRemoteAPI==false && SystemProperty.environment.value() != SystemProperty.Environment.Value.Production)
		{
			try
			{
//				Properties p = new Properties();
//				InputStream in = new FileInputStream("C:\\Universe (Non Repo)\\MySpacewarConfig\\db.properties");
//				p.load(in);
//				in.close();
		
				RemoteApiInstaller installer = new RemoteApiInstaller();
				installer.install(options);
			}
			catch(IllegalStateException ise)
			{
				// Ignore. Remote API is probably already installed
			}
			catch(Exception e)
			{
				// Ok fine, no remote API
				disableRemoteAPI=true;
				Logger.getLogger(getClass().toString()).log(Level.WARNING, "Failed to connect to remote API", e);
			}
		}

		mc = MemcacheServiceFactory.getMemcacheService();
		return mc;
	}


	public boolean isQueryCacheEnabled()
	{
		return queryModelCacheEnabled;
	}
	
	
	private void putEntityToMemcache(Entity entity)
	{
		mc.put(mcPrefix+entity.getKey().toString(), entity);		
	}
	
	protected void putEntitiesToMemcache(Iterable<Entity> entities)
	{
		Map<String, Entity> map = new HashMap<>();
		for(Entity entity:entities)
			map.put(mcPrefix+entity.getKey().toString(), entity);
		mc.putAll(map);
	}
	
	private void deleteEntityFromMemcache(Key entityKey)
	{
		mc.delete(mcPrefix+entityKey.toString());
	}
	
	private void deleteEntitiesFromMemcache(Collection<Key> entityKeys)
	{
		ArrayList<String> list = new ArrayList<>();
		for(Key key:entityKeys)
			list.add(mcPrefix+key.toString());
		mc.deleteAll(list);
	}
	
	private void markEntityChanged(Entity entity)
	{
		if (entity==null)
			return;
		
		// Here we're doing the check to see if we're in a transaction that the entity was fetched while in the transaction and not outside of it.
		// Note: we're excluding incomplete keys because that means the entity was just created and is not currently in the DB.
		if (isTransactionActive() && entity.getKey().isComplete()==true && enforceEntityFetchWithinTransaction)
			if (transactionallyFetchedEntities==null || transactionallyFetchedEntities.contains(entity.getKey())==false)
				throw new EntityNotFetchedWithinTransactionException(entity);
		
		if (transactionallyChangedEntities==null)
			transactionallyChangedEntities = new HashMap<>();
		
		transactionallyChangedEntities.put(entity.getKey(), entity);
		
		if (transactionallyDeletedEntities!=null)
			transactionallyDeletedEntities.remove(entity.getKey());
		
	}
	
	
	private void markEntityDeleted(Key key)
	{
		if (key==null)
			return;
		if (transactionallyDeletedEntities==null)
			transactionallyDeletedEntities = new HashSet<>();
		
		transactionallyDeletedEntities.add(key);
		
		if (transactionallyChangedEntities!=null)
			transactionallyChangedEntities.remove(key);
		
	}

	private void clearTransactionEntityTrackers()
	{
		if (transactionallyFetchedEntities!=null)
			transactionallyFetchedEntities.clear();
		if (transactionallyChangedEntities!=null)
			transactionallyChangedEntities.clear();
		if (transactionallyDeletedEntities!=null)
			transactionallyDeletedEntities.clear();
	}
	

	public void beginTransaction()
	{
		beginTransaction(false);
	}
	
	public void beginTransaction(boolean enforceEntityFetchWithinTransaction)
	{
		this.enforceEntityFetchWithinTransaction = enforceEntityFetchWithinTransaction;
		db.beginTransaction(TransactionOptions.Builder.withXG(true));
	}
	
	public void commit() throws ConcurrentModificationException
	{
		
		try
		{
			db.getCurrentTransaction().commit();

		}
		catch(ConcurrentModificationException cme)
		{
			// If the CacheDatastoreService is used after the transaction for some reason, it should stay safe to use, so we clear these entities
			clearTransactionEntityTrackers();
			throw cme;
		}

		
		if (transactionallyChangedEntities!=null)
			putEntitiesToMemcache(transactionallyChangedEntities.values());
		
		if (transactionallyDeletedEntities!=null)
			deleteEntitiesFromMemcache(transactionallyDeletedEntities);
		
		clearTransactionEntityTrackers();
	}
	
	public boolean isTransactionActive()
	{
		Transaction t = db.getCurrentTransaction(null);
		if (t!=null && t.isActive())
			return true;
		return false;
	}
	
	public boolean rollbackIfActive()
	{
		Transaction t = db.getCurrentTransaction(null);
		if (t!=null && t.isActive())
		{
			t.rollback();
			clearTransactionEntityTrackers();
			return true;
		}
		
		return false;
	}
	
	
	public void put(CachedEntity entity)
	{
		if (statsTracking)
			incrementStat("Stats_"+entity.getKey().getKind());
		
		Entity realEntity = entity.getEntity();
		if (cacheEnabled && isTransactionActive())
		{
			// If this is a new entity, then we need to add the entity to the transaction first
			if (entity.getKey().isComplete()==false)
			{
				db.put(realEntity);
				addEntityToTransaction(realEntity.getKey());
				markEntityChanged(realEntity);
			}
			else
			{
				markEntityChanged(realEntity);
				db.put(realEntity);
			}
				
		}
		else
			db.put(realEntity);

		
		if (cacheEnabled && isTransactionActive()==false)
			putEntityToMemcache(realEntity);
		
		entity.unsavedChanges = false;
	}
	
	/**
	 * If a transaction is active, the given entity will only be put just before
	 * the transaction is committed.
	 * 
	 * @param entity
	 */
	public void putOnCommit(CachedEntity entity)
	{
		//TODO: Implement this
		throw new RuntimeException("Method not implemented yet.");
	}

	public CachedEntity getIfExists(Key entityKey)
	{
		if (entityKey==null)
			return null;
		
		try 
		{
			return get(entityKey);
		} 
		catch (EntityNotFoundException e) 
		{
			return null;
		}
	}
	
	public CachedEntity refetch(CachedEntity entityToRefetchFromDB) 
	{
		if (entityToRefetchFromDB==null) return null;
		
		Key key = entityToRefetchFromDB.getKey();
		
		if (key.isComplete()==false)
			throw new IllegalArgumentException("The entity you are attempting to refetch hasn't even been saved to the DB yet as the key is incomplete.");
		try
		{
			return get(key);
		}
		catch(EntityNotFoundException ise)
		{
			throw new IllegalStateException("Entity "+entityToRefetchFromDB.getKey()+" was not found in the database.", ise);
		}
	}
	
	public List<CachedEntity> refetch(List<CachedEntity> entitiesToRefetchFromDB) 
	{
		if (entitiesToRefetchFromDB==null) return null;
		
		List<Key> keysToRefetchFromDB = new ArrayList<Key>();
		for(CachedEntity e:entitiesToRefetchFromDB)
		{
			if (e.getKey().isComplete()==false)
				throw new IllegalArgumentException("One of the entities you are attempting to refetch hasn't even been saved to the DB yet as the key is incomplete. Key="+e.getKey());
			keysToRefetchFromDB.add(e.getKey());
		}
		
		return fetchEntitiesFromKeys(keysToRefetchFromDB);
	}
	
	public CachedEntity get(Key entityKey) throws EntityNotFoundException
	{
		CachedEntity result;
		if (entityKey==null) return null;
		
		if (cacheEnabled && isTransactionActive()==false)
		{
			result = CachedEntity.wrap((Entity)mc.get(mcPrefix+entityKey.toString()));
			if (result==null)
			{
				result = CachedEntity.wrap(db.get(entityKey));
				if(result != null)
					mc.put(mcPrefix+entityKey.toString(), result.getEntity());
				if (statsTracking)
					incrementStat(DS_GETS);		// For statistics tracking of the cache's success
			}
			else
			{
				if (statsTracking)
					incrementStat(MC_GETS);		// For statistics tracking of the cache's success
			}
		}
		else
		{
			if (statsTracking)
				incrementStat(DS_GETS);		// For statistics tracking of the cache's success
			
			result = CachedEntity.wrap(db.get(entityKey));
			mc.put(mcPrefix+entityKey.toString(), result.getEntity());
			
			// If the transaction is active, we want to include this entity in the list of transactionally fetched entities
			addEntityToTransaction(entityKey);
			
		}
		
//		addEntityValuesToOldEntityValuesMap(result);	// So we can keep track of changes made to the fields on this entity
		return result;
	}

	private void addEntityToTransaction(Key entityKey) {
		if (isTransactionActive())
		{
			if (transactionallyFetchedEntities==null)
				transactionallyFetchedEntities = new HashSet<>();
			transactionallyFetchedEntities.add(entityKey);
		}
	}
	
	
	private List<Key> fetchKeys(Query q, int limit)
	{
		return fetchKeys(q, limit, null);
	}
	
	private List<Key> fetchKeys(Query q, int limit, Cursor startCursor)
	{
		q.setKeysOnly();
		
		prepareQuery(q);

		int chunkSize = 20;
		if (limit>500)
			chunkSize=100;
		if (limit>=1000)
			chunkSize=500;
		FetchOptions fo = FetchOptions.Builder.withLimit(limit).chunkSize(chunkSize);
		if (startCursor!=null)
			fo = fo.startCursor(startCursor);
		

		List<Key> keys = new ArrayList<>();
		QueryResultList<Entity> entities = pq.asQueryResultList(fo);
		
		int count = entities.size();
		if (count>limit)
			count = limit;
		
		for(int i = 0; i<count; i++)
		{
			Entity e = entities.get(i);
			keys.add(e.getKey());
		}
		
		lastQuery_endCursor = entities.getCursor();
		
		if (statsTracking)
			incrementStat(QUERYKEYCACHE_QUERIES);
		
		
		return keys;
	}

	private List<Key> fetchKeys(Query q, int limit, int offset)
	{
		q.setKeysOnly();
		
		prepareQuery(q);

		int chunkSize = 20;
		if (limit>500)
			chunkSize=100;
		if (limit>=1000)
			chunkSize=500;
		FetchOptions fo = FetchOptions.Builder.withLimit(limit).chunkSize(chunkSize).offset(offset);
		

		List<Key> keys = new ArrayList<>();
		QueryResultList<Entity> entities = pq.asQueryResultList(fo);

		int count = entities.size();
		if (count>limit)
			count = limit;
		
		for(int i = 0; i<count; i++)
		{
			Entity e = entities.get(i);
			keys.add(e.getKey());
		}
		
		lastQuery_endCursor = entities.getCursor();
		
		if (statsTracking)
			incrementStat(QUERYKEYCACHE_QUERIES);
		
		
		return keys;
	}
	
	public List<CachedEntity> fetchEntitiesFromKeys(Key...keys)
	{
		if (keys==null || keys.length==0)
			return null;
		
		return fetchEntitiesFromKeys(Arrays.asList(keys));
	}
	
	public List<CachedEntity> fetchEntitiesFromKeys(List<Key> keys)
	{
		//////////
		// First try to fetch all entities from memcache...

		// Fetch the entities from MC, but only if caching is turned on and there is no transaction currently active
		List<String> entityKeyStrings = new ArrayList<>();
		Map<String, Object> entitiesFromMC = null;
		if (cacheEnabled && isTransactionActive()==false)
		{
			for(Key key:keys)
				entityKeyStrings.add(mcPrefix+key.toString());
			entitiesFromMC = mc.getAll(entityKeyStrings);
		}
		
		if (entitiesFromMC!=null)
			if (statsTracking)
				incrementStat(QUERYKEYCACHE_MC_ENTITIES, entitiesFromMC.size());
		
		// Now check to see if we got all the entities we need...
		List<Key> keysThatStillNeedFetching = new ArrayList<>();
		for(Key key:keys)
		{
			String requiredKeyString = mcPrefix+key.toString();
			if (entitiesFromMC==null || entitiesFromMC.containsKey(requiredKeyString)==false)
			{
				// Oh, the memcache didn't have this entity, add it to the list we need to grab from the DB
				keysThatStillNeedFetching.add(key);
			}
		}
		
		// Now grab the missing entities from the DB...
		Map<Key,Entity> entitiesFromDB = null;
		if (keysThatStillNeedFetching.isEmpty()==false)
		{
			entitiesFromDB = db.get(keysThatStillNeedFetching);
			
			// Here we're going to keep track of the entities that were fetched while inside of the transaction. We will then throw 
			// later if we try to put an entity that wasn't fetched within the transaction.
			if (isTransactionActive())
			{
				if (transactionallyFetchedEntities==null)
					transactionallyFetchedEntities = new HashSet<>();
				transactionallyFetchedEntities.addAll(keysThatStillNeedFetching);
			}
			
			if (entitiesFromDB!=null) {
				incrementStat(QUERYKEYCACHE_DB_ENTITIES, entitiesFromDB.size());

				// Add these entities to memcache right away
				putEntitiesToMemcache(entitiesFromDB.values());
			}
		}
			

		// Now combine both lists into a single ordered result..
		List<CachedEntity> result = new ArrayList<>();
		for(Key key:keys)
		{
			CachedEntity mcEntity = null;
			CachedEntity dbEntity = null;
			if (entitiesFromMC!=null && entitiesFromMC.isEmpty()==false)
			{
				String keyString = mcPrefix+key.toString();
				mcEntity = CachedEntity.wrap((Entity)entitiesFromMC.get(keyString));
			}
			if (entitiesFromDB!=null && entitiesFromDB.isEmpty()==false)
			{
				dbEntity = CachedEntity.wrap(entitiesFromDB.get(key));
			}
			
			if (mcEntity!=null && dbEntity!=null)
				throw new IllegalStateException("Both the memcache and the datastore entities were fetched. This shouldn't ever happen.");
			
			if (mcEntity!=null)
			{
				result.add(mcEntity);
			}
			else if (dbEntity!=null)
			{
				result.add(dbEntity);
			}
			else
				result.add(null);
		}
		
		return result;
	}
	
	
	
	
	
	public CachedEntity fetchSingleEntity(Query q)
	{
		List<CachedEntity> entities = fetchAsList(q, 1);
		if (entities.isEmpty()) 
			return null;
		else
			return entities.get(0);
//		prepareQuery(q);
//		Entity result = pq.asSingleEntity();
//		
//		if (result!=null && q.isKeysOnly()==false)
//		{
//			mc.put(mcPrefix+result.getKey().toString(), result);
//		}
//		
//		incrementStat(QUERIES);		
//		incrementStat(QUERY_ENTITIES);
		
//		addEntityValuesToOldEntityValuesMap(result);	// So we can keep track of changes made to the fields on this entity
//		return result;
	}
	
	public List<Key> fetchAsList_Keys(String kind, Filter filter, int limit)
	{
		Query q = new Query(kind);
		q.setFilter(filter);
		q.setKeysOnly();
		return fetchKeys(q, limit);
	}
	
	public List<CachedEntity> fetchAsList(String kind, Filter filter, int limit, Cursor startEntityCursor)
	{
		Query q = new Query(kind);
		q.setFilter(filter);
		List<Key> keys = fetchKeys(q, limit, startEntityCursor);
		return fetchEntitiesFromKeys(keys);
	}
	
	
	public List<CachedEntity> fetchAsList(String kind, Filter filter, int limit)
	{
		Query q = new Query(kind);
		q.setFilter(filter);
		return fetchAsList(q, limit);
	}
	
	
	public List<CachedEntity> fetchAsList(Query q, int limit)
	{
		return fetchAsList(q, limit, null);
	}

	public List<CachedEntity> fetchAsList(Query q, int limit, Cursor startEntityCursor)
	{
		List<Key> keys = fetchKeys(q, limit, startEntityCursor);
		return fetchEntitiesFromKeys(keys);
	}

	public List<CachedEntity> fetchAsList(Query q, int limit, int offset)
	{
		List<Key> keys = fetchKeys(q, limit, offset);
		return fetchEntitiesFromKeys(keys);
	}

	
	

	public class CDSIterable implements Iterable<CachedEntity>
	{
		boolean noCache = false;
		Iterable<Entity> iterable;
		public CDSIterable(Iterable<Entity> iterable)
		{
			//noinspection unchecked
			this.iterable = iterable;
		}

		public CDSIterable(Iterable<Entity> iterable, boolean noCache)
		{
			//noinspection unchecked
			this.iterable = iterable;
			this.noCache = noCache;
		}

		public class CDSIterator implements Iterator<CachedEntity>
		{
			boolean noCache = false;
			Iterator<Entity> iterator;
			
			public CDSIterator(Iterator<Entity> iterator, boolean noCache)
			{
				this.noCache = noCache;
				this.iterator = iterator;
			}
			
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public CachedEntity next() {
				CachedEntity e = CachedEntity.wrap(iterator.next());
				if (noCache==false)
					putEntityToMemcache(e.getEntity());
				return e;
			}

			@Override
			public void remove() {
				iterator.remove();
			}
			
		}
		
		@Override
		public Iterator<CachedEntity> iterator() {
			return new CDSIterator(iterable.iterator(), noCache);
		}
		
	}
	
	
	public Iterable<CachedEntity> fetchAsIterable(Query q) {
		
		prepareQuery(q);
		
		return new CDSIterable(pq.asIterable(FetchOptions.Builder.withChunkSize(500)));
	}
	
	public Iterable<CachedEntity> fetchAsIterable(Query q, boolean noCache) {
		
		prepareQuery(q);
		
		return new CDSIterable(pq.asIterable(FetchOptions.Builder.withChunkSize(500)), noCache);
	}
	
	public Iterable<CachedEntity> fetchAsIterable(Query q, int offset) {
		
		prepareQuery(q);
		
		return new CDSIterable(pq.asIterable(FetchOptions.Builder.withChunkSize(500).offset(offset)));
	}
	
	public Object prepareQuery(Query q)
	{
		pq = db.prepare(q);
		
		return null;
	}

	public KeyRange allocateIds(Key parentKey, String kind, int num) {
		return db.allocateIds(parentKey, kind, num);
	}

	public KeyRange allocateIds(String kind, int num) {
		return db.allocateIds(kind, num);
	}

	public long countEntities(Query query) {
		return db.prepare(query).countEntities(FetchOptions.Builder.withDefaults());
	}
	
	
	
	public void delete(CachedEntity entity)
	{
		if (entity==null)
			return;
		delete(entity.getKey());
	}

	public void deleteByCachedEntities(List<CachedEntity> list)
	{
		if (list==null || list.isEmpty())
			return; 
		
		List<Key> keys = new ArrayList<>();
		for(CachedEntity e:list)
			keys.add(e.getKey());
		
		delete(keys);
	}
	
	public void delete(List<Key> keys)
	{
		if (keys==null || keys.isEmpty())
			return;
		
		db.delete(keys);
		
		if (cacheEnabled && isTransactionActive())
		{
			for(Key key:keys)
				markEntityDeleted(key);
		}
		else
		{
			deleteEntitiesFromMemcache(keys);
		}
		
	}
	
	public void delete(Key entityKey)
	{
		if (entityKey==null)
			return;
		
		
		db.delete(entityKey);
		
		if (cacheEnabled && isTransactionActive())
			markEntityDeleted(entityKey);
		else
			deleteEntityFromMemcache(entityKey);
		
//		reportDeletedEntity(entityKey);
	}
	

	public Cursor getLastQueryCursor()
	{
		return lastQuery_endCursor;
	}
	
	
	
	
	protected Long incrementStat(String statKey)
	{
		return incrementStat(statKey, 1l);
	}
	
	protected Long incrementStat(String statKey, long amount)
	{
		return mc.increment(statKey, amount);
	}
	
	protected Long incrementStat(String statKey, long amount, Long initialValue)
	{
		return mc.increment(statKey, amount, initialValue);
	}
	
	protected Long incrementStat(String statKey, long amount, Long initialValue, int expirySeconds)
	{
		Long previousValue = (Long)mc.get(statKey);
		if (previousValue==null)
		{
			if (initialValue==null)
				initialValue=0l;
			mc.put(statKey, initialValue+amount, Expiration.byDeltaSeconds(expirySeconds));
			return initialValue+amount;
		}
		else
		{
			mc.put(statKey,previousValue+amount, Expiration.byDeltaSeconds(expirySeconds));
			return previousValue+amount;
		}
	}
	
	protected double incrementStat(String statKey, double amount)
	{
		while(true)
		{
			IdentifiableValue identifiable = mc.getIdentifiable(statKey);
			if (identifiable==null)
			{
				throw new InvalidValueException("Cannot increment a stat that starts from null.");
			}
			else
			{
				double newValue = (((Double)identifiable.getValue())+amount);
				boolean success = mc.putIfUntouched(statKey, identifiable, newValue);
				if (success)
					return newValue;
			}
		}
	}
	
	public void setStatDouble(String statKey, double value)
	{
		mc.put(statKey, value);
	}
	
	public void setStat(String statKey, Long value)
	{
		mc.put(statKey, value);
	}
	
	public void setStat(String statKey, Long value, int expirySeconds)
	{
		mc.put(statKey, value, Expiration.byDeltaSeconds(expirySeconds));
	}
	
	public Long getStat(String statKey)
	{
		return (Long)mc.get(statKey);
	}

	public Double getStatDouble(String statKey)
	{
		return (Double)mc.get(statKey);
	}

	public void clearStats() {
		mc.put(MC_GETS, 0l);
		mc.put(DS_GETS, 0l);
		mc.put(QUERIES, 0l);
		mc.put(QUERY_ENTITIES, 0l);
		mc.put(MC_QUERY_ENTITIES, 0l);
		mc.put(MC_QUERIES, 0l);
		
		mc.put(QUERYKEYCACHE_QUERIES, 0l);
		mc.put(QUERYKEYCACHE_DB_ENTITIES, 0l);
		mc.put(QUERYKEYCACHE_MC_ENTITIES, 0l);
		
		
		
		String prefix = "Stats_";
		CachedSchema schema = SchemaInitializer.getSchema();
		if (schema!=null)
		{
			List<String> allKinds = schema.getKinds();
			for(String kind:allKinds)
			{
				mc.put(prefix+kind, 0l);
			}
		}
		
	}

	/**
	 * This is supposed to be an efficient memcache-backed counter.
	 * It uses dual memcache storing in the hopes that it wont be cleared as often. 
	 * @param counterName
	 * @param change
	 * @param initialValue
	 */
	public Long changeCounter(String counterName, long change, Long initialValue)
	{
		String backupName = "counterBackup-"+counterName;
		counterName = "counter-"+counterName;
		try
		{
			Long currentValue = incrementStat(counterName, change);
			setStat(backupName, currentValue);
			return currentValue;
		}
		catch(InvalidValueException e)
		{
			Long backupValue = getStat(backupName);
			
			if (backupValue!=null)
			{
				backupValue+=change;
				setStat(counterName, backupValue);
				setStat(backupName, backupValue);
				return backupValue;
			}
			else
			{
				initialValue+=change;
				setStat(counterName, initialValue);
				setStat(backupName, initialValue);
				return initialValue;
			}
		}
	}
	

	/**
	 * Just like the other changeCounter() but this one accepts an expiry in seconds.
	 * 
	 * @param counterName
	 * @param change
	 * @param initialValue
	 * @param expirySeconds
	 * @return
	 */
	public Long changeCounter(String counterName, long change, Long initialValue, int expirySeconds)
	{
		String backupName = "counterBackup-"+counterName;
		counterName = "counter-"+counterName;
		try
		{
			Long currentValue = incrementStat(counterName, change, initialValue, expirySeconds);
			setStat(backupName, currentValue, expirySeconds);
			return currentValue;
		}
		catch(InvalidValueException e)
		{
			Long backupValue = getStat(backupName);
			
			if (backupValue!=null)
			{
				backupValue+=change;
				setStat(counterName, backupValue, expirySeconds);
				setStat(backupName, backupValue, expirySeconds);
				return backupValue;
			}
			else
			{
				initialValue+=change;
				setStat(counterName, initialValue, expirySeconds);
				setStat(backupName, initialValue, expirySeconds);
				return initialValue;
			}
		}
	}
	
	public Long getCounter(String counterName, Long initialValue)
	{
		String backupName = "counterBackup-"+counterName;
		counterName = "counter-"+counterName;

		Long value = getStat(counterName);
		if (value==null)
		{
			value = getStat(backupName);
			if (value==null)
			{
				if (initialValue!=null)
				{
					setStat(counterName, initialValue);
					setStat(backupName, initialValue);
					return initialValue;
				}
				else
				{
					return null;
				}
			}
			else
			{
				setStat(counterName, value);
				return value;
			}
		}
		return value;
	}
	
	/**
	 * This is supposed to be an efficient memcache-backed counter.
	 * It uses dual memcache storing in the hopes that it wont be cleared as often. 
	 * @param counterName
	 * @param change
	 * @param initialValue
	 */
	public Double changeCounter(String counterName, double change, Double initialValue)
	{
		String backupName = "counterBackup-"+counterName;
		counterName = "counter-"+counterName;
		try
		{
			Double currentValue = incrementStat(counterName, change);
			setStatDouble(backupName, currentValue);
			return currentValue;
		}
		catch(InvalidValueException e)
		{
			Double backupValue = getStatDouble(backupName);
			
			if (backupValue!=null)
			{
				backupValue+=change;
				setStatDouble(counterName, backupValue);
				setStatDouble(backupName, backupValue);
				return backupValue;
			}
			else
			{
				initialValue+=change;
				setStatDouble(counterName, initialValue);
				setStatDouble(backupName, initialValue);
				return initialValue;
			}
		}
	}
	
	public Double getCounter(String counterName, Double initialValue)
	{
		String backupName = "counterBackup-"+counterName;
		counterName = "counter-"+counterName;

		Double value = getStatDouble(counterName);
		if (value==null)
		{
			value = getStatDouble(backupName);
			if (value==null)
			{
				if (initialValue!=null)
				{
					setStatDouble(counterName, initialValue);
					setStatDouble(backupName, initialValue);
					return initialValue;
				}
				else
				{
					return null;
				}
			}
			else
			{
				setStatDouble(counterName, value);
				return value;
			}
		}
		return value;
	}

	public boolean flagActionLimiter(String actionName, int periodInSeconds, long maximumActions)
	{
		return flagActionLimiter(actionName, periodInSeconds, maximumActions, null);
	}

	
	
	/**
	 * This handy method will use memcache to keep track of actions taken that should be done in moderation.
	 * It is common to use this method to limit things like: 
	 *  - The number of verification emails a particular IP is allowed to resend in a given time period
	 *  - The number of signups a given IP is allowed to have in a given time period
	 *  - The number of chat messages a given user is allowed to send in a given time period
	 *  These are just exmaples.
	 * 
	 * Returns TRUE if the action has reached it's limit and should be limited. 
	 * 
	 * @param actionName
	 * @param periodInSeconds
	 * @param maximumActions
	 * @return True = the action must now be limited. False = The action should be allowed to continue.
	 */
	public boolean flagActionLimiter(String actionName, int periodInSeconds, long maximumActions, Integer penaltyDuration)
	{
		Long counter = (Long)mc.get("actionLimiter-"+actionName);
		if (counter==null)
		{
			mc.put("actionLimiter-"+actionName, maximumActions, Expiration.byDeltaSeconds(periodInSeconds));
			counter = maximumActions;
		}
		else
		{
			counter = mc.increment("actionLimiter-"+actionName, -1l);
		}
		
		if (counter==null || counter>0)
			return false;
		else
		{
			if (penaltyDuration!=null)
				mc.put("actionLimiter-"+actionName, 0L, Expiration.byDeltaSeconds(penaltyDuration));
			
			return true;
		}
	}
	
	public boolean isActionLimited(String actionName)
	{
		Long counter = (Long)mc.get("actionLimiter-"+actionName);
		if (counter==null || counter>0l)
			return true;
		
		return false;
	}

	
	
	///////////////////////////////////////
	// Safer memcache stuff
	// The purpose of this mechanism is to store memcache values in multiple locations so that clearing happens less often
		

	public void setSaferMemcacheValue(String key, Serializable value, int backups)
	{	
		if (key==null) throw new IllegalArgumentException("key cannot be null.");
		if (backups<0 || backups>=5) throw new IllegalArgumentException("Backup count must be between 0 and 4.");
		MemcacheService mc = getMC();
		
		SaferMCValueWrapper wrappedValue = new SaferMCValueWrapper(value);
		mc.put(key, wrappedValue);
		for(int i = 0; i<backups; i++)
			mc.put(key+"-backup#"+i, wrappedValue);
	}
	
	public Object getSaferMemcacheValue(String key, int backups)
	{
		if (key==null) throw new IllegalArgumentException("key cannot be null.");
		if (backups<0 || backups>=5) throw new IllegalArgumentException("Backup count must be between 0 and 4.");
		
		SaferMCValueWrapper wrappedValue = (SaferMCValueWrapper)mc.get(key);
		if (wrappedValue==null)
			for(int i = 0; i<backups; i++)
			{
				
				wrappedValue = (SaferMCValueWrapper)mc.get(key+"-backup#"+i);
				if (wrappedValue!=null)
					break;
			}
		
		if (wrappedValue==null)
			return null;
		else
			return wrappedValue.value;
	}
	
	
	
	////////////////////////////
	// QUERY CACHE FUNCTIONS

//	Map<String, Map<String,Object>> oldEntityValues = new HashMap<String, Map<String,Object>>();
//
//	/**
//	 * This is supposed to be called every time an entity is get() from the database or memcache. We need it
//	 * because we use the "oldEntityValues" to compare field values on put to know which fields were changed
//	 * and what they were changed to. This is necessary to properly invalidate QueryModels. 
//	 * @param list
//	 */
//	protected void addEntityValuesToOldEntityValuesMap(List<Entity> list)
//	{
//		for(Entity entity:list)
//			addEntityValuesToOldEntityValuesMap(entity);
//	}
//	
//	protected void addEntityValuesToOldEntityValuesMap(Entity entity)
//	{
//		if (entity==null) return;
//		Map<String,Object> copy = new HashMap<String,Object>();
//		copy.putAll(entity.getProperties());
//		oldEntityValues.put(entity.getKey().toString(), copy);
//	}
//	
//	private void reportNewEntity(Entity entity)
//	{
//		for(String fieldName:entity.getProperties().keySet())
//		{
//			reportEntityPropertyChange(entity, fieldName);
//		}
//	}
//	
//	private void reportDeletedEntity(Key entity)
//	{
//		if (queryModelCacheEnabled==false)
//			return;
//		Map<String,Object> oldValues = oldEntityValues.get(entity.toString());
//		if (oldValues==null) throw new IllegalStateException("Entity "+entity+" did not have it's old property values stored.");
//		
//		for(String fieldName:oldValues.keySet())
//		{
//			reportEntityPropertyChange(entity.getKind(), fieldName, oldValues.get(fieldName));
//		}
//	}
//	
//	private void detectAndReportEntityPropertyChange(Entity entity)
//	{
//		if (queryModelCacheEnabled==false)
//			return;
//		Map<String,Object> oldValues = oldEntityValues.get(entity.getKey().toString());
//		if (oldValues==null) throw new IllegalStateException("Entity "+entity+" did not have it's old property values stored.");
//		
//		for(String fieldName:entity.getProperties().keySet())
//		{
//			if (ObjectUtils.equals(entity.getProperty(fieldName), oldValues)==false)
//				reportEntityPropertyChange(entity, fieldName);
//		}
//	}
//	
//	private void reportEntityPropertyChange(String kind, String fieldName, Object value, Object oldValue)
//	{
//		Set<String> queryModelIds_oldValues = null;
//
//		String qmfKeyOld = QueryModel.generateQueryModelFilterKey(kind, fieldName, oldValue);
//		queryModelIds_oldValues = getQueryModelIdsFor(qmfKeyOld);
//		
//		String qmfKey = QueryModel.generateQueryModelFilterKey(kind, fieldName, value);
//		
//		Set<String> queryModelIds = getQueryModelIdsFor(qmfKey);
//		queryModelIds.addAll(queryModelIds_oldValues);
//		
//		// Now we're going to delete the queryModels that were invalidated due to this change...
//		deleteQueryModels(queryModelIds);
//	}
//	
//	
//	private void reportEntityPropertyChange(String kind, String fieldName, Object value)
//	{
//		String qmfKey = QueryModel.generateQueryModelFilterKey(kind, fieldName, value);
//		Set<String> queryModelIds = getQueryModelIdsFor(qmfKey);
//		
//		// Now we're going to delete the queryModels that were invalidated due to this change...
//		deleteQueryModels(queryModelIds);
//	}
//	
//	
//	private void reportEntityPropertyChange(Entity entity, String fieldName)
//	{
//		Map<String,Object> oldValues = oldEntityValues.get(entity.getKey().toString());
//		
//		
//		String kind = entity.getKind();
//		Object value = entity.getProperty(fieldName);
//		if (oldValues!=null)
//		{
//			Object oldValue = oldValues.get(fieldName);
//			reportEntityPropertyChange(kind, fieldName, value, oldValue);
//		}
//		else
//			reportEntityPropertyChange(kind, fieldName, value);
//			
//	}
//	
//	private void deleteQueryModels(Set<String> queryModelIds)
//	{
////		Iterator<String> iterator = queryModelIds.iterator();
////		while(iterator.hasNext())
////		{
////			if (clearedQueryModelIds.contains(iterator.next()))
////				iterator.remove();
////		}
//		
////		if (queryModelIds.size()>0)
////		{
//			mc.deleteAll(queryModelIds);
////			clearedQueryModelIds.addAll(queryModelIds);
////		}
//		
//	}
//	
//	private Set<String> getQueryModelIdsFor(String qmfKey)
//	{
//		@SuppressWarnings("unchecked")
//		Set<String> queryModels = (Set<String>)mc.get(qmfKey);
//		if (queryModels==null)
//			queryModels = new HashSet<String>();
//		
//		return queryModels;
//	}
	
	

	
	
	

	
	
	
	
	///////////////////////////////////////////
	// Different memcache functions
	
	
	@SuppressWarnings("unchecked")
	/**
	 * 
	 * @param key
	 * @param objectToAdd
	 * @return True if the objectToAdd was in fact added to the set and was NOT already there
	 */
	public boolean addToSet_MC(String key, Object objectToAdd)
	{
		while(true)
		{
			Set<Object> set;
			IdentifiableValue identifiable = mc.getIdentifiable(key);
			if (identifiable==null)
				set = new HashSet<>();
			else
				set = (Set<Object>)identifiable.getValue();

			// If this object is already in the set, then don't bother adding it and just get out
			if (set.contains(objectToAdd))
				return false;
			
			set.add(objectToAdd);
			
			if (identifiable==null)
			{
				boolean success = mc.put(key, set, null, SetPolicy.ADD_ONLY_IF_NOT_PRESENT);
				if (success) return true;
			}
			else
			{
				boolean success = mc.putIfUntouched(key, identifiable, set);
				if (success) return true;
			}
		}
	}
	public Set<Object> getSet_MC(String key)
	{
		Set<Object> set = (Set<Object>)mc.get(key);
		return set;
	}
	
	@SuppressWarnings("unchecked")
	/**
	 * 
	 * @param key
	 * @param objectToDelete
	 * @return True if the objectToDelete actually needed to be deleted
	 */
	public boolean deleteFromSet_MC(String key, Object objectToDelete)
	{
		while(true)
		{
			Set<Object> set;
			IdentifiableValue identifiable = mc.getIdentifiable(key);
			if (identifiable==null)
				return false;
			else
				set = (Set<Object>)identifiable.getValue();

			// If this object is already deleted from the set, then don't bother and just get out
			if (set.contains(objectToDelete)==false)
				return false;
			
			set.remove(objectToDelete);
			
			boolean success = mc.putIfUntouched(key, identifiable, set);
			if (success) return true;
		}
	}
	
	
	
	
	
	
	
	
	
	
	
}
