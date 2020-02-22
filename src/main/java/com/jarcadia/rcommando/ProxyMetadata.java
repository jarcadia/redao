package com.jarcadia.rcommando;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JavaType;
import com.jarcadia.rcommando.exception.RedisCommandoException;
import com.jarcadia.rcommando.proxy.DaoProxy;

class ProxyMetadata {
	
	protected enum Type {
		DEFAULT, GETTER, SETTER, CLEARER, PASSTHROUGH, IMPLEMENTED;
	}
	
	private final Class<? extends DaoProxy> proxyClass;
	private final Method daoGetter;
	private final Map<Method, Type> typeMap;
	private final List<Getter> getters;
	private final Map<String, Method> fieldToGetterMap;
	private final String[] getterFieldNames;
	private final Map<Method, String[]> setterFieldsMap;
	private final Map<Method, String[]> clearerFieldsMap;
	private final Map<Method, Method> passthroughMethodsMap;
	private final MethodHandles.Lookup lookup;

	public ProxyMetadata(Class<? extends DaoProxy> proxyClass, Method daoGetter, Set<Method> defaultMethods,
			List<Getter> getters, Map<Method, String[]> setters, Map<Method, String[]> clearers,
			Map<Method, Method> passthroughMethodsMap, MethodHandles.Lookup lookup) {
		this.proxyClass = proxyClass;
		this.daoGetter = daoGetter;
		this.getters = List.copyOf(getters);
		this.fieldToGetterMap = Map.copyOf(getters.stream().collect(Collectors.toMap(Getter::getFieldName, Getter::getMethod)));
		this.getterFieldNames = getters.stream().map(Getter::getFieldName).collect(Collectors.toList()).toArray(new String[0]);
		this.setterFieldsMap = Map.copyOf(setters);
		this.clearerFieldsMap = Map.copyOf(clearers);
		this.passthroughMethodsMap = passthroughMethodsMap;
		this.lookup = lookup;
		
		this.typeMap = new HashMap<>();
		typeMap.put(daoGetter, Type.IMPLEMENTED);
		
		for (Method method : defaultMethods) {
			typeMap.put(method, Type.DEFAULT);
		}
		
		for (Getter getter : getters) {
			typeMap.put(getter.getMethod(), Type.GETTER);
		}
		
		for (Method setter : setters.keySet()) {
			typeMap.put(setter, Type.SETTER);
		}

		for (Method clearer : clearers.keySet()) {
			typeMap.put(clearer, Type.CLEARER);
		}
		
		for (Method proxyMethod : passthroughMethodsMap.keySet()) {
			typeMap.put(proxyMethod, Type.PASSTHROUGH);
		}
	}
	
	protected Class<? extends DaoProxy> getProxyClass() {
		return proxyClass;
	}

	protected Type getType(Method method) {
		Type type = typeMap.get(method);
		if (type == null) {
			throw new RedisCommandoException("Proxy method " + proxyClass.getName() + "." + method.getName() + " is not supported");
		}
		return type;
	}
	
	protected Method getDaoGetter() {
		return daoGetter;
	}

	protected List<Getter> getGetters() {
		return getters;
	}

	public String[] getGetterFieldNames() {
		return getterFieldNames;
	}
	
	protected String[] getSetterFields(Method setter) {
		return setterFieldsMap.get(setter);
	}

	protected String[] getClearerFields(Method clearer) {
		return clearerFieldsMap.get(clearer);
	}
	
	protected Method getObjectMethod(Method proxyMethod) {
		return passthroughMethodsMap.get(proxyMethod);
	}

	protected Method getGetter(String fieldName) {
		return fieldToGetterMap.get(fieldName);
	}
	
	protected MethodHandles.Lookup getLookup() {
		return this.lookup;
	}

	protected static class Getter {

		private final Method method;
		private final String fieldName;
		private final JavaType returnType;

		Getter(Method method, String fieldName, JavaType returnType) {
			this.method = method;
			this.fieldName = fieldName;
			this.returnType = returnType;
		}
		
		public Method getMethod() {
			return method;
		}

		public String getFieldName() {
			return fieldName;
		}

		public JavaType getReturnType() {
			return returnType;
		}
	}
}
