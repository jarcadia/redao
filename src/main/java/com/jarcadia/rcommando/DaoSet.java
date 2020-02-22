package com.jarcadia.rcommando;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.lettuce.core.StreamScanCursor;
import io.lettuce.core.output.ValueStreamingChannel;

public class DaoSet implements Iterable<Dao> {

    private final RedisCommando rcommando;
    private final ValueFormatter formatter;
    private final String setKey;

    protected DaoSet(RedisCommando rcommando, ValueFormatter formatter, String setKey) {
        this.rcommando = rcommando;
        this.formatter = formatter;
        this.setKey = setKey;
    }
    
    public String getKey() {
    	return this.getKey();
    }

    public long size() {
        return rcommando.core().scard(setKey);
    }

    public boolean has(String id) {
        return rcommando.core().sismember(setKey, id);
    }
    
    public Dao get(String id) {
        return new Dao(rcommando, formatter, setKey, id);
    }
    
    public Dao randomMember() {
    	String id = rcommando.core().srandmember(setKey);
    	return id == null ? null : get(id);
    }

    @Override
    public Iterator<Dao> iterator() {
        return new RedisObjectStreamer();
    }
    
    public Stream<Dao> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }
    
    public Set<Dao> getSubset(Collection<String> ids) {
    	return ids.stream().map(id -> this.get(id))
    			.collect(Collectors.toSet());
    }
    
    private class RedisObjectStreamer implements Iterator<Dao> {

        private final List<String> buffer;
        private final ValueStreamingChannel<String> channel;
        private StreamScanCursor cursor;
        
        public RedisObjectStreamer() {
            this.buffer = new LinkedList<>();
            this.channel = value -> buffer.add(value);
            this.cursor = rcommando.core().sscan(channel, setKey);
        }

        @Override
        public boolean hasNext() {
        	if (!buffer.isEmpty()) {
        		return true;
        	} else if (cursor.isFinished()) {
        		return false;
        	} else {
                cursor = rcommando.core().sscan(channel, setKey, cursor);
                return this.hasNext();
        	}
        }

        @Override
        public Dao next() {
            return get(buffer.remove(0));
        }
    }
}
