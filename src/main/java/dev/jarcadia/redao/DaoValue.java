package dev.jarcadia.redao;

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
import dev.jarcadia.redao.exception.RcDeserializationException;
import dev.jarcadia.redao.exception.RedisCommandoException;

public class DaoValue {
    
    private final ValueFormatter formatter;
    private final String fieldName;
    private final String value;
    
    protected DaoValue(ValueFormatter formatter, String fieldName, String value) {
        this.formatter = formatter;
        this.fieldName = fieldName;
        this.value = value;
    }

    public String getFieldName() {
        return fieldName;
    }
    
    public boolean isPresent() {
        return value != null;
    }

    public String asString() {
        try {
			return formatter.deserialize(value, String.class);
		} catch (RcDeserializationException e) {
			throw new RedisCommandoException("Unable to deserialize " + this.getRawValue() + " as String");
		}
    }

    public int asInt() {
        try {
			return formatter.deserialize(value, Integer.class);
		} catch (RcDeserializationException e) {
			throw new RedisCommandoException("Unable to deserialize " + this.getRawValue() + " as Integer");
		}
    }

    public long asLong() {
        try {
			return formatter.deserialize(value, Long.class);
		} catch (RcDeserializationException e) {
			throw new RedisCommandoException("Unable to deserialize " + this.getRawValue() + " as Long");
		}
    }
    
    public double asDouble() {
        try {
			return formatter.deserialize(value, Double.class);
		} catch (RcDeserializationException e) {
			throw new RedisCommandoException("Unable to deserialize " + this.getRawValue() + " as Double");
		}
    }
    
    public <T> T as(Class<T> clazz) {
        try {
			return formatter.deserialize(value, clazz);
		} catch (RcDeserializationException e) {
			throw new RedisCommandoException("Unable to deserialize " + this.getRawValue() + " as "+ clazz.getSimpleName());
		}
    }
    
    public <T> T as(JavaType type) {
        try {
			return formatter.deserialize(value, type);
		} catch (RcDeserializationException e) {
			throw new RedisCommandoException("Unable to deserialize " + this.getRawValue() + " as "+ type.getTypeName());
		}
    }
    
    public <T> Optional<T> asOptionalOf(Class<T> clazz) {
    	try {
    		if (value == null) {
    			return null;
    		} else {
    			T v = formatter.deserialize(value, clazz);
    			if ( v == null) {
    				throw new RedisCommandoException("Deserializing " + value + " as " + clazz.getName() + " resulted in null");
    			} else {
    				return Optional.of(v);
    			}
    		}
		} catch (RcDeserializationException e) {
			throw new RedisCommandoException("Unable to deserialize " + this.getRawValue() + " as Optional<"+ clazz.getSimpleName()+">");
		}
    }
    
    public <T> List<T> asListOf(Class<T> clazz) {
        CollectionType typeReference = TypeFactory.defaultInstance().constructCollectionType(List.class, clazz);
        try {
			return formatter.deserialize(value, typeReference);
		} catch (RcDeserializationException e) {
			throw new RedisCommandoException("Unable to deserialize " + this.getRawValue() + " as List<"+ clazz.getSimpleName()+">");
		}
    }
    
    public <T> Set<T> asSetOf(Class<T> clazz) {
        CollectionType typeReference = TypeFactory.defaultInstance().constructCollectionType(Set.class, clazz);
        try {
			return formatter.deserialize(value, typeReference);
		} catch (RcDeserializationException e) {
			throw new RedisCommandoException("Unable to deserialize " + this.getRawValue() + " as Set<"+ clazz.getSimpleName()+">");
		}
    }
    
    public Map<String, DaoValue> asMap() {
        ObjectNode obj = (ObjectNode) formatter.asNode(value);
        Map<String, DaoValue> result = new HashMap<>();
        
        for (Iterator<Entry<String, JsonNode>> iter = obj.fields(); iter.hasNext(); ) {
            Entry<String, JsonNode> entry = iter.next();
            String fieldName = entry.getKey();
            String rawValue = formatter.serialize(entry.getValue());
            result.put(fieldName, new DaoValue(formatter, fieldName, rawValue));
        }
        
        return result;
    }
    
    public String getRawValue() {
        return this.value;
    }
}
