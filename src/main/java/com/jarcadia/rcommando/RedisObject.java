package com.jarcadia.rcommando;

import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class RedisObject {
    
    private final RedisCommando rcommando;
    private final RedisValueFormatter formatter;
    private final String setKey;
    private final String hashKey;
    private final String id;
    
    public RedisObject(RedisCommando rcommando, RedisValueFormatter formatter, String setKey, String id) {
        this.rcommando = rcommando;
        this.formatter = formatter;
        this.setKey = setKey;
        this.hashKey = setKey + "." + id;
        this.id = id;
    }
    
    public String getId() {
        return id;
    }
    
    public long getVersion() {
        return this.get("v").asLong();
    }
    
    public RedisValue get(String field) {
        return new RedisValue(formatter, rcommando.core().hget(hashKey, field));
    }
    
    public RedisValues get(String... fields) {
        RedisValues values = new RedisValues(formatter);
        rcommando.core().hmget(values.getChannel(), this.hashKey, fields);
        return values;
    }

    public void touch() {
        rcommando.eval()
            .useScriptFile("objTouch")
            .addKeys(this.setKey, this.hashKey)
            .returnStatus();
    }

    public void set(Object... fieldsAndValues) {
        rcommando.eval()
                .useScriptFile("objSet")
                .addKeys(this.setKey, this.hashKey)
                .addArgs(prepareArgs(fieldsAndValues))
                .returnStatus();
    }
    
    public List<RedisChangedValue> checkedSet(Object... fieldsAndValues) {
        String jsonChanges = rcommando.eval()
                .useScriptFile("objCheckedSet")
                .addKeys(this.setKey, this.hashKey, this.setKey+".change")
                .addArgs(prepareArgs(fieldsAndValues))
                .returnStatus();
        
        JavaType type = TypeFactory.defaultInstance().constructCollectionType(List.class, String[].class);
        List<String[]> changes = formatter.deserialize(jsonChanges, type);
        return changes.stream()
                .map(change -> new RedisChangedValue(change[0], new RedisValue(formatter, change[1]), new RedisValue(formatter, change[2])))
                .collect(Collectors.toList());
    }
    
    public int checkedDelete() {
        return rcommando.eval()
                .useScriptFile("objCheckedDelete")
                .addKeys(this.setKey, this.hashKey, this.setKey + ".change")
                .addArgs(this.id)
                .returnInt();
    }
    
    private String[] prepareArgs(Object[] fieldsAndValues) {
        String[] args = new String[fieldsAndValues.length];
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("A value must be specified for each field name");
        }
        for (int i=0; i<args.length; i++) {
            if (i % 2 == 0) {
                // Process field
                if (fieldsAndValues[i] instanceof String) {
                    args[i] = (String) fieldsAndValues[i];
                } else {
                    throw new IllegalArgumentException("Field name is set operation must be a String");
                }
            } else {
                // Process value
                args[i] = formatter.smartSerailize(fieldsAndValues[i]);
            }
        }
        return args;
    }
}
