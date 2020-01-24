package com.jarcadia.rcommando;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.lettuce.core.KeyValue;

public class RcCountDownLatch {
    
    private final RedisCommando rcommando;
    private final RedisValueFormatter formatter;
    private final String hashKey;
    
    public RcCountDownLatch(RedisCommando rcommando, RedisValueFormatter formatter, String id) {
        this.rcommando = rcommando;
        this.formatter = formatter;
        this.hashKey =  "cdl." + id;
    }
    
    public void init(int count, Object... fieldsAndValues) {
        Map<String, String> args = prepareArgs(fieldsAndValues);
        args.put("remaining",  String.valueOf(count));
        rcommando.core().hmset(this.hashKey, args);
    }

    public boolean decrement() {
        long remaining = rcommando.core().hincrby(this.hashKey, "remaining", -1);
        if (remaining == 0) {
            rcommando.core().del(this.hashKey);
            return true;
        } else {
            return false;
        }
    }

    public Optional<RcValue> decrement(String field) {
        long remaining = rcommando.core().hincrby(this.hashKey, "remaining", -1);
        if (remaining == 0) {
            RcValue value = new RcValue(formatter, rcommando.core().hget(hashKey, field));
            rcommando.core().del(this.hashKey);
            return Optional.of(value);
        }
        return Optional.empty();
    }

    public Optional<RcValues> decrement(String... fields) {
        long remaining = rcommando.core().hincrby(this.hashKey, "remaining", -1);
        if (remaining == 0) {
            List<KeyValue<String, String>> values = rcommando.core().hmget(this.hashKey, fields);
            rcommando.core().del(this.hashKey);
            return Optional.of(new RcValues(formatter, values.iterator()));
        }
        return Optional.empty();
    }
    
    private Map<String, String> prepareArgs(Object[] fieldsAndValues) {
        if (fieldsAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("A value must be specified for each field name");
        }
        Map<String, String> args = new HashMap<>();
        for (int i=0; i<fieldsAndValues.length; i+=2) {
            if (!(fieldsAndValues[i] instanceof String)) {
                throw new IllegalArgumentException("Field name must be a String");
            }
            args.put((String) fieldsAndValues[i], formatter.serialize(fieldsAndValues[i+1]));
        }
        return args;
    }
}
