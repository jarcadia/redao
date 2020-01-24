package com.jarcadia.rcommando;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class RcObjectMapper extends ObjectMapper {
	
	public RcObjectMapper(RedisCommando rcommando) {
		this.registerModules(rcObjectModule(rcommando), rcMapModule(rcommando));
	}
	
	private SimpleModule rcObjectModule(RedisCommando rcommando) {
		SimpleModule rcObjModule = new SimpleModule();
		rcObjModule.addSerializer(new RcObjSerializer());
		rcObjModule.addDeserializer(RcObject.class, new RcObjDeserializer(rcommando));
		return rcObjModule;
	}
	
	private SimpleModule rcMapModule(RedisCommando rcommando) {
		SimpleModule module = new SimpleModule();
		module.addSerializer(new RcMapSerializer());
		module.addDeserializer(RcSet.class, new RcMapDeserializer(rcommando));
		return module;
	}
}
