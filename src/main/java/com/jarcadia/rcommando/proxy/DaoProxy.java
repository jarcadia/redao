package com.jarcadia.rcommando.proxy;

import com.jarcadia.rcommando.Dao;

public interface DaoProxy {
	
	public String getSetKey();
	public String getId();
	public boolean exists();
	public boolean delete();
	
	/**
	 * <b> Warning! Use of the Proxy's underlying Dao is generally discouraged. Modifications to the data using 
	 * this Dao will <b>NOT</b> be reflected in the Proxy's get methods.
	 * 
	 * The primary use case for this is usually rooted in getting/setting fields by name when the name is only known
	 * at run-time.
	 * 
	 * @return The Dao that is proxied by this DaoProxy
	 */
	public Dao getDao();

}
