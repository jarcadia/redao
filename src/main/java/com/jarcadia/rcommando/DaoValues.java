package com.jarcadia.rcommando;

import java.util.Iterator;

import io.lettuce.core.KeyValue;

public class DaoValues {
    
    private final ValueFormatter formatter;
    private final Iterator<KeyValue<String, String>> iter;
    
    protected DaoValues(ValueFormatter formatter, Iterator<KeyValue<String, String>> iter) {
        this.formatter = formatter;
        this.iter = iter;
    }
    
    public DaoValue next() {
        KeyValue<String, String> val = iter.next();
        return new DaoValue(formatter, val.getValueOrElse(null));
    }
    
    public boolean hasNext() {
    	return iter.hasNext();
    }
}
