package com.jarcadia.rcommando;

import java.util.Iterator;

import io.lettuce.core.KeyValue;

public class RedisValues {
    
    private final RedisValueFormatter formatter;
    private final Iterator<KeyValue<String, String>> iter;
    
    protected RedisValues(RedisValueFormatter formatter, Iterator<KeyValue<String, String>> iter) {
        this.formatter = formatter;
        this.iter = iter;
    }
    
    public RedisValue next() {
        KeyValue<String, String> val = iter.next();
        return new RedisValue(formatter, val.getValue());
    }
}
