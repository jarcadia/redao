package com.jarcadia.rcommando;

import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class RedisValue {
    
    private final RedisValueFormatter formatter;
    private final String value;
    
    protected RedisValue(RedisValueFormatter formatter, String value) {
        this.formatter = formatter;
        this.value = value;
    }
    
    public String asString() {
        return value;
    }
    
    public int asInt() {
        return Integer.parseInt(value);
    }

    public long asLong() {
        return Long.parseLong(value);
    }
    
    public double asDouble() {
        return Double.parseDouble(value);
    }
    
    public <T> T as(Class<T> clazz) {
        return formatter.deserialize(value, clazz);
    }
    
    public <T> List<T> asListOf(Class<T> clazz) {
        CollectionType typeReference = TypeFactory.defaultInstance().constructCollectionType(List.class, clazz);
        return formatter.deserialize(value, typeReference);
    }
}
