package dev.jarcadia.redao;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamScanCursor;
import io.lettuce.core.output.ScoredValueStreamingChannel;

public class Index implements Iterable<Dao> {

    private final RedaoCommando rcommando;
    private final ValueFormatter formatter;
    private final String type;

    protected Index(RedaoCommando rcommando, ValueFormatter formatter, String type) {
        this.rcommando = rcommando;
        this.formatter = formatter;
        this.type = type;
    }
    
    public String getType() {
    	return this.getType();
    }

    public long count() {
        return rcommando.core().zcard(type);
    }

    public boolean has(String id) {
        return rcommando.core().zscore(type, id) != null;
    }
    
    public Dao get(String id) {
        return new Dao(rcommando, formatter, type, id);
    }

    public Dao get() {
        return new Dao(rcommando, formatter, type, UUID.randomUUID().toString());
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
            this.cursor = rcommando.core().zscan(channel, type);
        }

        @Override
        public boolean hasNext() {
        	if (!buffer.isEmpty()) {
        		return true;
        	} else if (cursor.isFinished()) {
        		return false;
        	} else {
                cursor = rcommando.core().zscan(channel, type, cursor);
                return this.hasNext();
        	}
        }

        @Override
        public Dao next() {
            return get(buffer.remove(0).getValue());
        }
    }
}
