package com.jarcadia.rcommando;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class RcMapSerializer extends StdSerializer<RcSet> {
	
    public RcMapSerializer() {
        super(RcSet.class);
    }
 
	@Override
	public void serialize(RcSet value, JsonGenerator gen, SerializerProvider provider) throws IOException {
		 gen.writeStartObject();
		 gen.writeStringField("mapKey", value.getKey());
	     gen.writeEndObject();
	}
}
