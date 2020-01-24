package com.jarcadia.rcommando;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class RedisValueFormatter {

    private final ObjectMapper mapper;

    public RedisValueFormatter(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    protected <T> T deserialize(String json, Class<T> clazz) throws RcDeserializationException {
        try {
            return mapper.readValue(json, clazz);
        }
        catch (IOException e) {
            throw new RcDeserializationException("Unable to deserialize JSON to " + clazz.getSimpleName(), e);
        }
    }
    
    protected <T> T deserialize(String json, TypeReference<T> typeRef) throws RcDeserializationException {
        try {
            return mapper.readValue(json, typeRef);
        }
        catch (IOException e) {
            throw new RcDeserializationException("Unable to deserialize JSON", e);
        }
    }
    
    protected <T> T deserialize(String json, JavaType type) throws RcDeserializationException {
        try {
            return mapper.readValue(json, type);
        }
        catch (IOException e) {
            throw new RcDeserializationException("Unable to deserialize JSON", e);
        }
    }
    
    protected JsonNode asNode(String json) {
        try {
            return mapper.readTree(json);
        }
        catch (IOException e) {
            throw new RcException("Unable to parse JSON", e);
        }
    }
    
    protected String serialize(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        }
        catch (JsonProcessingException e) {
            throw new RcException("Unable to serialize to JSON", e);
        }
    }
}
