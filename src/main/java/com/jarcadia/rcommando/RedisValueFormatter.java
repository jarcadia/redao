package com.jarcadia.rcommando;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RedisValueFormatter {

    private final ObjectMapper mapper;

    public RedisValueFormatter(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    protected <T> T deserialize(String json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        }
        catch (IOException e) {
            throw new RedisException("Unable to deserialize JSON to " + clazz.getSimpleName(), e);
        }
    }
    
    protected <T> T deserialize(String json, TypeReference<T> typeRef) {
        try {
            return mapper.readValue(json, typeRef);
        }
        catch (IOException e) {
            throw new RedisException("Unable to deserialize JSON", e);
        }
    }
    
    protected <T> T deserialize(String json, JavaType type) {
        try {
            return mapper.readValue(json, type);
        }
        catch (IOException e) {
            throw new RedisException("Unable to deserialize JSON", e);
        }
    }
    
    protected String serialize(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        }
        catch (JsonProcessingException e) {
            throw new RedisException("Unable to serialize to JSON", e);
        }
    }
    
    protected String smartSerailize(Object value) {
        if (value instanceof String) {
            return (String) value;
        } else if (value instanceof Integer) {
            return String.valueOf((Integer) value);
        } else if (value instanceof Long) {
            return String.valueOf((Long) value);
        } else if (value instanceof Double) {
            return String.valueOf((Double) value);
        } else if (value instanceof Enum) {
            return ((Enum<?>) value).name();
        } else {
            return this.serialize(value);
        }
    }
}
