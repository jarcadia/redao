package dev.jarcadia.redao.proxy;

import dev.jarcadia.redao.Dao;

public interface Proxy {
	
	String getType();
	String getId();
	boolean exists();
	boolean delete();
	<T extends Proxy> T as(Class<T> proxyClass);

	/**
	 * <b> Warning! Use of the Proxy's underlying Dao is generally discouraged. Modifications to the data using 
	 * this Dao will <b>NOT</b> be reflected in the Proxy's get methods.
	 * 
	 * The primary use case for this is usually getting/setting fields that have dynamic names only known
	 * at run-time.
	 * 
	 * @return The Dao that is proxied by this Proxy
	 */
	Dao getDao();

}
