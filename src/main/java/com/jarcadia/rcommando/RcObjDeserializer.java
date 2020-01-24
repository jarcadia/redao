package com.jarcadia.rcommando;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class RcObjDeserializer extends StdDeserializer<RcObject> {
	
	private RedisCommando rcommando;
	
	public RcObjDeserializer(RedisCommando rcommando) {
		super(RcObject.class);
		this.rcommando = rcommando;
    }
 
    @Override
    public RcObject deserialize(JsonParser parser, DeserializationContext deserializer) throws IOException {
    	JsonNode node = parser.readValueAsTree();
    	if (node.isObject()) {
    		JsonNode setKey = node.get("setKey");
    		JsonNode id = node.get("id");
    		if (setKey != null && id != null
    				&& !setKey.isNull() && !id.isNull()
    				&& setKey.isTextual() && id.isTextual()) {
    			return rcommando.getSetOf(setKey.asText()).get(id.asText());
    		}
    	}
    	return null;
    }
}
