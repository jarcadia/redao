package com.jarcadia.rcommando;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class RcMapDeserializer extends StdDeserializer<RcSet> {
	
	private RedisCommando rcommando;
	
	public RcMapDeserializer(RedisCommando rcommando) {
		super(RcSet.class);
		this.rcommando = rcommando;
    }
 
    @Override
    public RcSet deserialize(JsonParser parser, DeserializationContext deserializer) throws IOException {
    	JsonNode node = parser.readValueAsTree();
    	if (node.isObject()) {
    		JsonNode mapKey = node.get("mapKey");
    		if (mapKey != null && !mapKey.isNull() && mapKey.isTextual()) {
    			return rcommando.getSetOf(mapKey.asText());
    		}
    	}
    	return null;
    }
}
