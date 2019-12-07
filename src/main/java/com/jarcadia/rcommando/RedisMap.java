package com.jarcadia.rcommando;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.lettuce.core.StreamScanCursor;
import io.lettuce.core.output.ValueStreamingChannel;

public class RedisMap implements Iterable<RedisObject> {

    private final RedisCommando rcommando;
    private final RedisValueFormatter formatter;
    private final String setKey;

    protected RedisMap(RedisCommando rcommando, RedisValueFormatter formatter, String setKey) {
        this.rcommando = rcommando;
        this.formatter = formatter;
        this.setKey = setKey;
    }

    public long size() {
        return rcommando.core().scard(setKey);
    }

    @Override
    public Iterator<RedisObject> iterator() {
        return new RedisObjectStreamer();
    }
    
    public Stream<RedisObject> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }

    public RedisObject get(String id) {
        return new RedisObject(rcommando, formatter, setKey, id);
    }
    
    private class RedisObjectStreamer implements Iterator<RedisObject> {

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
        public RedisObject next() {
            if (buffer.isEmpty()) {
                cursor = rcommando.core().sscan(channel, setKey, cursor);
            }
            return get(buffer.remove(0));
        }
        
    }
}
