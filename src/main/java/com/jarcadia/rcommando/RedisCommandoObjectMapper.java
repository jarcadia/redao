package com.jarcadia.rcommando;

import java.io.IOException;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.jarcadia.rcommando.proxy.DaoProxy;

public class RedisCommandoObjectMapper extends ObjectMapper {
	
	public RedisCommandoObjectMapper(RedisCommando rcommando) {
		this.registerModules(rcProxyModule(), optionalModule(), rcObjectModule(rcommando), rcMapModule(rcommando));
	}
	
	public <T extends DaoProxy> void registerProxyClass(Class<T> proxyClass) {
		SimpleModule module = new SimpleModule();
		JsonDeserializer<T> deserializer = new StdDeserializer<T>(proxyClass) {
			@Override
			public T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
				Dao object = p.readValueAs(Dao.class);
				return object.as(proxyClass);
			}
		};
		module.addDeserializer(proxyClass, deserializer); 
		this.registerModule(module);
	}
	
	private SimpleModule optionalModule() {
		SimpleModule module = new SimpleModule();
		module.addSerializer(new OptionalSerializer());
		module.addDeserializer(Optional.class, new OptionalDeserializer());
		return module;
		
	}
	private SimpleModule rcObjectModule(RedisCommando rcommando) {
		SimpleModule rcObjModule = new SimpleModule();
		rcObjModule.addSerializer(new DaoSerializer());
		rcObjModule.addDeserializer(Dao.class, new DaoDeserializer(rcommando));
		return rcObjModule;
	}
	
	private SimpleModule rcMapModule(RedisCommando rcommando) {
		SimpleModule module = new SimpleModule();
		module.addSerializer(new DaoSetSerializer());
		module.addDeserializer(DaoSet.class, new DaoSetDeserializer(rcommando));
		return module;
	}
	
	private SimpleModule rcProxyModule() {
		SimpleModule module = new SimpleModule();
		module.addSerializer(new DaoProxySerializer());
		return module;
	}
	
	private class DaoProxySerializer extends StdSerializer<DaoProxy> {
		
		 public DaoProxySerializer() {
            super(DaoProxy.class);
        }
     
        @Override
        public void serialize(DaoProxy value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        	gen.writeString(value.getSetKey() + ":" + value.getId());
        }
	}
	

	private class DaoSerializer extends StdSerializer<Dao> {
		
	    public DaoSerializer() {
	        super(Dao.class);
	    }
	 
		@Override
		public void serialize(Dao value, JsonGenerator gen, SerializerProvider provider) throws IOException {
			gen.writeString(value.getSetKey() + ":" + value.getId());
		}
	}

	
	private class DaoDeserializer extends StdDeserializer<Dao> {
		
		private RedisCommando rcommando;
		
		public DaoDeserializer(RedisCommando rcommando) {
			super(Dao.class);
			this.rcommando = rcommando;
	    }
	 
	    @Override
	    public Dao deserialize(JsonParser parser, DeserializationContext deserializer) throws IOException {
	    	JsonNode node = parser.readValueAsTree();
	    	if (node.isTextual()) {
	    		String str = node.asText();
	    		int index = str.indexOf(":");
	    		if (index > 0) {
                    String setKey = str.substring(0, index);
                    String id = str.substring(index + 1);
                    return rcommando.getSetOf(setKey).get(id);
	    		} else {
                    throw new JsonParseException(parser, "Unable to deserialize Dao. Expected String of form setKey:id but got " + str);
	    		}
	    	} else {
	    		throw new JsonParseException(parser, "Unable to deserialize Dao. Expected String of form setKey:id but got non-text node: " + node.toString());
	    	}
	    }
	}
	
	private class DaoSetSerializer extends StdSerializer<DaoSet> {
		
	    public DaoSetSerializer() {
	        super(DaoSet.class);
	    }
	 
		@Override
		public void serialize(DaoSet value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeString(value.getKey());
		}
	}
	
	private class DaoSetDeserializer extends StdDeserializer<DaoSet> {
		
		private RedisCommando rcommando;
		
		public DaoSetDeserializer(RedisCommando rcommando) {
			super(DaoSet.class);
			this.rcommando = rcommando;
	    }
	 
	    @Override
	    public DaoSet deserialize(JsonParser parser, DeserializationContext deserializer) throws IOException {
	    	JsonNode node = parser.readValueAsTree();
	    	if (node.isTextual()) {
	    		return rcommando.getSetOf(node.asText());
	    	} else {
	    		throw new JsonParseException(parser, "Unable to deserialze DaoSet. Expected String setKey");
	    	}
	    }
	}
	
	private class OptionalSerializer extends StdSerializer<Optional<?>> {

	    public OptionalSerializer() {
	        super(Optional.class, false);
	    }
	    
		@Override
		public void serialize(Optional<?> value, JsonGenerator gen, SerializerProvider provider) throws IOException {
			if (value.isPresent()) {
				gen.writeObject(value.get());
			} else {
				gen.writeNull();
			}
		}
	}
	
	private static class OptionalDeserializer extends JsonDeserializer<Optional<?>> implements ContextualDeserializer {

	    private final JavaType innerType;
	    
	    OptionalDeserializer() {
	    	innerType = null;
	    }
	    
	    OptionalDeserializer(JavaType innerType) {
	    	this.innerType = innerType;
		}

	    @Override
	    public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) throws JsonMappingException {
	    	JavaType type = ctxt.getContextualType().containedType(0);
	        return new OptionalDeserializer(type);
	    }

	    @Override
	    public Optional<?> deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {
	    	return Optional.ofNullable(ctxt.readValue(parser, innerType));
	    }
	}
}
