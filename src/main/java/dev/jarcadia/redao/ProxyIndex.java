package dev.jarcadia.redao;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import dev.jarcadia.redao.proxy.Proxy;

public class ProxyIndex<T extends Proxy> implements Iterable<T> {

    private final Index sourceIndex;
    private final Class<T> proxyClass;

    protected ProxyIndex(Index sourceIndex, Class<T> proxyClass) {
    	this.sourceIndex = sourceIndex;
    	this.proxyClass = proxyClass;
    }
    
    public String getKey() {
    	return sourceIndex.getType();
    }

    public long size() {
    	return sourceIndex.count();
    }

    public boolean has(String id) {
    	return sourceIndex.has(id);
    }
    
    public T get(String id) {
        return sourceIndex.get(id).as(proxyClass);
    }
    
    @Override
    public Iterator<T> iterator() {
        return new ProxiedObjectIterator(sourceIndex.iterator());
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
