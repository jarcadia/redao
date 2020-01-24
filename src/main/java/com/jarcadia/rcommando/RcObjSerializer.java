package com.jarcadia.rcommando;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class RcObjSerializer extends StdSerializer<RcObject> {
	
    public RcObjSerializer() {
        super(RcObject.class);
    }
 
	@Override
	public void serialize(RcObject value, JsonGenerator gen, SerializerProvider provider) throws IOException {
		 gen.writeStartObject();
		 gen.writeStringField("setKey", value.getSetKey());
	     gen.writeStringField("id", value.getId());
	     gen.writeEndObject();
	}
}
