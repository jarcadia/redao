package com.jarcadia.rcommando.proxy;

import com.jarcadia.rcommando.Dao;

public interface Proxy {
	
	public String getSetKey();
	public String getId();
	public boolean exists();
	public boolean delete();
	
	/**
	 * <b> Warning! Use of the Proxy's underlying Dao is generally discouraged. Modifications to the data using 
	 * this Dao will <b>NOT</b> be reflected in the Proxy's get methods.
	 * 
	 * The primary use case for this is usually getting/setting fields dynamically when the name is only known
	 * at run-time.
	 * 
	 * @return The Dao that is proxied by this Proxy
	 */
	public Dao getDao();

}
