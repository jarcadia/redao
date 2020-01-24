package com.jarcadia.rcommando;

import java.util.Iterator;

import io.lettuce.core.KeyValue;

public class RcValues {
    
    private final RedisValueFormatter formatter;
    private final Iterator<KeyValue<String, String>> iter;
    
    protected RcValues(RedisValueFormatter formatter, Iterator<KeyValue<String, String>> iter) {
        this.formatter = formatter;
        this.iter = iter;
    }
    
    public RcValue next() {
        KeyValue<String, String> val = iter.next();
        return new RcValue(formatter, val.getValue());
    }
}
