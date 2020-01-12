package com.jarcadia.rcommando;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class RedisObjectSerializer extends JsonSerializer<RedisObject> {

    @Override
    public void serialize(RedisObject value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("mapKey", value.getMapKey());
        gen.writeStringField("id", value.getId());
        gen.writeEndObject();
    }

}
