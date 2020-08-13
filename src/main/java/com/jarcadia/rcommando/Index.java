package com.jarcadia.rcommando;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamScanCursor;
import io.lettuce.core.output.ScoredValueStreamingChannel;

public class Index implements Iterable<Dao> {

    private final RedisCommando rcommando;
    private final ValueFormatter formatter;
    private final String setKey;

    protected Index(RedisCommando rcommando, ValueFormatter formatter, String setKey) {
        this.rcommando = rcommando;
        this.formatter = formatter;
        this.setKey = setKey;
    }
    
    public String getKey() {
    	return this.getKey();
    }

    public long count() {
        return rcommando.core().zcard(setKey);
    }

    public boolean has(String id) {
        return rcommando.core().zscore(setKey, id) != null;
    }
    
    public Dao get(String id) {
        return new Dao(rcommando, formatter, setKey, id);
    }

    public Set<Dao> get(Collection<String> ids) {
        // TODO this could be improved to be one (or a few) round trips using LUA
        return ids.stream().map(id -> this.get(id))
                .collect(Collectors.toSet());
    }

    @Override
    public Iterator<Dao> iterator() {
        return new IndexIterator();
    }
    
    public Stream<Dao> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }
    
    private class IndexIterator implements Iterator<Dao> {

        private final List<ScoredValue<String>> buffer;
        private final ScoredValueStreamingChannel<String> channel;
        private StreamScanCursor cursor;
        
        public IndexIterator() {
            this.buffer = new LinkedList<>();
            this.channel = value -> buffer.add(value);
            this.cursor = rcommando.core().zscan(channel, setKey);
        }

        @Override
        public boolean hasNext() {
        	if (!buffer.isEmpty()) {
        		return true;
        	} else if (cursor.isFinished()) {
        		return false;
        	} else {
                cursor = rcommando.core().zscan(channel, setKey, cursor);
                return this.hasNext();
        	}
        }

        @Override
        public Dao next() {
            return get(buffer.remove(0).getValue());
        }
    }
}
