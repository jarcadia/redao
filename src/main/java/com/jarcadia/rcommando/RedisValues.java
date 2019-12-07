package com.jarcadia.rcommando;

import java.util.HashMap;
import java.util.Map;

import io.lettuce.core.output.KeyValueStreamingChannel;

public class RedisValues {
    
    private final Map<String, RedisValue> values;
    private final KeyValueStreamingChannel<String, String> channel;
    
    protected RedisValues(RedisValueFormatter formatter) {
        this.values = new HashMap<>();
        this.channel = (key, value) -> values.put(key, new RedisValue(formatter, value));
    }
    
    protected KeyValueStreamingChannel<String, String> getChannel() {
        return this.channel;
    }
    
    public RedisValue get(String field) {
        return values.get(field);
    }
}
