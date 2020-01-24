package com.jarcadia.rcommando;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class RcValue {
    
    private final RedisValueFormatter formatter;
    private final String value;
    
    protected RcValue(RedisValueFormatter formatter, String value) {
        this.formatter = formatter;
        this.value = value;
    }
    
    public boolean isPresent() {
        return value != null;
    }

    public String asString() {
        try {
			return formatter.deserialize(value, String.class);
		} catch (RcDeserializationException e) {
			throw new RcException("Unable to deserialize " + this.getRawValue() + " as String");
		}
    }

    public int asInt() {
        try {
			return formatter.deserialize(value, Integer.class);
		} catch (RcDeserializationException e) {
			throw new RcException("Unable to deserialize " + this.getRawValue() + " as Integer");
		}
    }

    public long asLong() {
        try {
			return formatter.deserialize(value, Long.class);
		} catch (RcDeserializationException e) {
			throw new RcException("Unable to deserialize " + this.getRawValue() + " as Long");
		}
    }
    
    public double asDouble() {
        try {
			return formatter.deserialize(value, Double.class);
		} catch (RcDeserializationException e) {
			throw new RcException("Unable to deserialize " + this.getRawValue() + " as Double");
		}
    }
    
    public <T> T as(Class<T> clazz) {
        try {
			return formatter.deserialize(value, clazz);
		} catch (RcDeserializationException e) {
			throw new RcException("Unable to deserialize " + this.getRawValue() + " as "+ clazz.getSimpleName());
		}
    }
    
    public <T> T as(JavaType type) {
        try {
			return formatter.deserialize(value, type);
		} catch (RcDeserializationException e) {
			throw new RcException("Unable to deserialize " + this.getRawValue() + " as "+ type.getTypeName());
		}
    }
    
    
    public <T> Optional<T> asOptionalOf(Class<T> clazz) {
    	try {
			return value == null ? Optional.empty() : Optional.of(formatter.deserialize(value, clazz));
		} catch (RcDeserializationException e) {
			throw new RcException("Unable to deserialize " + this.getRawValue() + " as Optional<"+ clazz.getSimpleName()+">");
		}
    }
    
    public <T> List<T> asListOf(Class<T> clazz) {
        CollectionType typeReference = TypeFactory.defaultInstance().constructCollectionType(List.class, clazz);
        try {
			return formatter.deserialize(value, typeReference);
		} catch (RcDeserializationException e) {
			throw new RcException("Unable to deserialize " + this.getRawValue() + " as List<"+ clazz.getSimpleName()+">");
		}
    }
    
    public <T> Set<T> asSetOf(Class<T> clazz) {
        CollectionType typeReference = TypeFactory.defaultInstance().constructCollectionType(Set.class, clazz);
        try {
			return formatter.deserialize(value, typeReference);
		} catch (RcDeserializationException e) {
			throw new RcException("Unable to deserialize " + this.getRawValue() + " as Set<"+ clazz.getSimpleName()+">");
		}
    }
    
    public Map<String, RcValue> asMap() {
        ObjectNode obj = (ObjectNode) formatter.asNode(value);
        Map<String, RcValue> result = new HashMap<>();
        
        for (Iterator<Entry<String, JsonNode>> iter = obj.fields(); iter.hasNext(); ) {
            Entry<String, JsonNode> entry = iter.next();
            String fieldName = entry.getKey();
            String rawValue = formatter.serialize(entry.getValue());
            result.put(fieldName, new RcValue(formatter, rawValue));
        }
        
        return result;
    }
    
    public String getRawValue() {
        return this.value;
    }
}
