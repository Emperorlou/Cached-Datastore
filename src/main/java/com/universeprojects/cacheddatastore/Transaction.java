package com.universeprojects.cacheddatastore;

import java.util.ConcurrentModificationException;


/**
 * This class handles the usual use case for transactions. It is built specifically to be used 
 * with the CachedDatastoreService as the supporting data access service. It has features to 
 * make writing transactions easier and safer. For example, if an entity is put to the database
 * while in the transaction, but the entity was not fetched while in the transaction, an exception
 * will be thrown.
 * 
 * @author Owner
 *
 */
public abstract class Transaction<T>
{
	private CachedDatastoreService ds;
	
	public Transaction(CachedDatastoreService ds) 
	{
		this.ds = ds;
	}
	
	public abstract T doTransaction(CachedDatastoreService ds) throws AbortTransactionException;

	/**
	 * Call this method when you want to run the transaction.
	 * @throws AbortTransactionException 
	 */
	public T run() throws AbortTransactionException
	{
		T result = null;
		
		boolean success = false;
		while(success == false)
		{
			try
			{
				if (disableTransaction()==false)
					ds.beginTransaction(true);
				
				result = doTransaction(ds);
				success = true;
				
				if (disableTransaction()==false)
					ds.commit();
			}
			catch(ConcurrentModificationException cme)
			{
				// Ignore the exception, we will be automatically retrying
			}
			finally
			{
				ds.rollbackIfActive();
			}
		}
		
		return result;
	}
	
	
	/**
	 * Use this method to enter entities into the transaction that were fetched outside of the transaction.
	 * 
	 * @param entity
	 * @return
	 */
	public CachedEntity refetch(CachedEntity entity)
	{
		return ds.refetch(entity);
	}
	
	public boolean disableTransaction()
	{
		return false;
	}
	
}
