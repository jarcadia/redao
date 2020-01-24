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

public class RcSet implements Iterable<RcObject> {

    private final RedisCommando rcommando;
    private final RedisValueFormatter formatter;
    private final String setKey;

    protected RcSet(RedisCommando rcommando, RedisValueFormatter formatter, String setKey) {
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

    @Override
    public Iterator<RcObject> iterator() {
        return new RedisObjectStreamer();
    }
    
    public Stream<RcObject> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }

    public RcObject get(String id) {
        return new RcObject(rcommando, formatter, setKey, id);
    }
    
    public Set<RcObject> getSubset(Collection<String> ids) {
    	return ids.stream().map(id -> this.get(id))
    			.collect(Collectors.toSet());
    }
    
    private class RedisObjectStreamer implements Iterator<RcObject> {

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
            return !buffer.isEmpty() || !cursor.isFinished();
        }

        @Override
        public RcObject next() {
            if (buffer.isEmpty()) {
                cursor = rcommando.core().sscan(channel, setKey, cursor);
            }
            return get(buffer.remove(0));
        }
        
    }
}
