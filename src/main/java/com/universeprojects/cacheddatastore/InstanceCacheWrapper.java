package com.universeprojects.cacheddatastore;

import java.io.Serializable;
import java.util.Date;

public class InstanceCacheWrapper implements Serializable
{
	private static final long serialVersionUID = 3222354118091068864L;

	final public Object data;
	final public Date expiry;
	
	public InstanceCacheWrapper(Object data, Date expiry)
	{
		this.data = data;
		this.expiry = expiry;
	}

	@Override
	public boolean equals(Object obj)
	{
		boolean equal = super.equals(obj);
		if (equal==false)
		{
			if (obj instanceof InstanceCacheWrapper)
			{
				InstanceCacheWrapper o = (InstanceCacheWrapper)obj;
				if (o.data==null && this.data==null) return true;
				if (o.data==null) return false;
				equal = o.data.equals(this.data);
			}
		}
		return equal;
	}

	@Override
	public int hashCode()
	{
		if (data==null) return super.hashCode();
		return data.hashCode();
	}

}
