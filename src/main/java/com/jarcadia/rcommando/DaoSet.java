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
        return rcommando.core().zcard(setKey);
    }

    public boolean has(String id) {
        return rcommando.core().zscore(setKey, id) != null;
    }
    
    public Dao get(String id) {
        return new Dao(rcommando, formatter, setKey, id);
    }
    
    public Dao randomMember() {
        // TODO make this random
    	List<String> range = rcommando.core().zrange(setKey, 0, 1);
    	return range.size() == 0 ? null : get(range.get(0));
    }

    @Override
    public Iterator<Dao> iterator() {
        return new DaoIterator();
    }
    
    public Stream<Dao> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }
    
    public Set<Dao> getSubset(Collection<String> ids) {
    	return ids.stream().map(id -> this.get(id))
    			.collect(Collectors.toSet());
    }
    
    private class DaoIterator implements Iterator<Dao> {

        private final List<ScoredValue<String>> buffer;
        private final ScoredValueStreamingChannel<String> channel;
        private StreamScanCursor cursor;
        
        public DaoIterator() {
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
