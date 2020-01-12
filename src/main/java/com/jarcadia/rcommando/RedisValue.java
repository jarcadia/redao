package com.jarcadia.rcommando;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class RedisValue {
    
    private final RedisValueFormatter formatter;
    private final String value;
    
    protected RedisValue(RedisValueFormatter formatter, String value) {
        this.formatter = formatter;
        this.value = value;
    }
    
    public boolean isNull() {
        return value == null;
    }

    public String asString() {
        return formatter.deserialize(value, String.class);
    }

    public Optional<String> asOptionalString() {
        return value == null ? Optional.empty() : Optional.of(formatter.deserialize(value, String.class));
    }

    public int asInt() {
        return formatter.deserialize(value, Integer.class);
    }

    public long asLong() {
        return formatter.deserialize(value, Long.class);
    }
    
    public double asDouble() {
        return formatter.deserialize(value, Double.class);
    }
    
    public <T> T as(Class<T> clazz) {
        return formatter.deserialize(value, clazz);
    }
    
    public <T> List<T> asListOf(Class<T> clazz) {
        CollectionType typeReference = TypeFactory.defaultInstance().constructCollectionType(List.class, clazz);
        return formatter.deserialize(value, typeReference);
    }
    
    public List<RedisObject> asObjectList(RedisMap map) {
        CollectionType typeReference = TypeFactory.defaultInstance().constructCollectionType(List.class, String.class);
        List<String> ids = formatter.deserialize(value, typeReference);
        return ids.stream().map(id -> map.get(id)).collect(Collectors.toList());
    }
    
    public <T> Set<T> asSetOf(Class<T> clazz) {
        CollectionType typeReference = TypeFactory.defaultInstance().constructCollectionType(Set.class, clazz);
        return formatter.deserialize(value, typeReference);
    }
    
    public Map<String, RedisValue> asMap() {
        ObjectNode obj = (ObjectNode) formatter.asNode(value);
        Map<String, RedisValue> result = new HashMap<>();
        
        for (Iterator<Entry<String, JsonNode>> iter = obj.fields(); iter.hasNext(); ) {
            Entry<String, JsonNode> entry = iter.next();
            String fieldName = entry.getKey();
            String rawValue = formatter.serialize(entry.getValue());
            result.put(fieldName, new RedisValue(formatter, rawValue));
        }
        
        return result;
    }
    
    public String getRawValue() {
        return this.value;
    }
}
