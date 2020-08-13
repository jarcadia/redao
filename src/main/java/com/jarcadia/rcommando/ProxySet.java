package com.jarcadia.rcommando;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.jarcadia.rcommando.proxy.Proxy;

public class ProxySet<T extends Proxy> implements Iterable<T> {

    private final Index set;
    private final Class<T> proxyClass;

    protected ProxySet(Index set, Class<T> proxyClass) {
    	this.set = set;
    	this.proxyClass = proxyClass;
    }
    
    public String getKey() {
    	return set.getKey();
    }

    public long size() {
    	return set.count();
    }

    public boolean has(String id) {
    	return set.has(id);
    }
    
    public T get(String id) {
        return set.get(id).as(proxyClass);
    }
    
    @Override
    public Iterator<T> iterator() {
        return new ProxiedObjectIterator(set.iterator());
    }
    
    public Stream<T> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }
    
    public Set<T> getSubset(Collection<String> ids) {
    	return ids.stream().map(id -> this.get(id))
    			.collect(Collectors.toSet());
    }
    
    private class ProxiedObjectIterator implements Iterator<T> {
    	
    	private final Iterator<Dao> source;

        public ProxiedObjectIterator(Iterator<Dao> source) {
        	this.source = source;
        }

        @Override
        public boolean hasNext() {
        	return source.hasNext();
        }

        @Override
        public T next() {
        	return source.next().as(proxyClass);
        }
        
    }
}
