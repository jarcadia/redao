package com.jarcadia.rcommando;

import java.beans.Introspector;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jarcadia.rcommando.ProxyMetadata.Getter;
import com.jarcadia.rcommando.exception.ProxyException;
import com.jarcadia.rcommando.proxy.Proxy;
import com.jarcadia.rcommando.proxy.Internal;

public class ProxyMetadataFactory {

	private final Logger logger = LoggerFactory.getLogger(RedisCommando.class);
	
	private final ObjectMapper objectMapper;
	
	protected ProxyMetadataFactory(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}
	
	protected ProxyMetadata create(Class<? extends Proxy> proxyClass) {
		Method lookupGenerator = null;
		Method daoGetter = null;

		Set<Method> defaultMethods = new HashSet<>();
		Map<Method, Method> passthroughMethodsMap = new HashMap<>();

		List<Getter> getters = new LinkedList<>();
		Map<Method, String[]> setterFieldsMap = new HashMap<>();
		Map<Method, String[]> clearerFieldsMap = new HashMap<>();


		for (Method method : proxyClass.getMethods()) {
			if (isStatic(method)) {
				if (isLookupGenerator(method)) {
					lookupGenerator = method;
				} else {
					throw new ProxyException("Static proxy methods are not supported");
				}
			} else {
				if (method.isDefault()) {
					defaultMethods.add(method);
				} else if (isSetKeyGetter(method)) {
					passthroughMethodsMap.put(method, getPassthroughMethod(method));
				} else if (isIdGetter(method)) {
					passthroughMethodsMap.put(method, getPassthroughMethod(method));
				} else if (isDaoGetter(method)) {
					daoGetter = method;
				} else if (isGetter(method)) {
					String fieldName = getFieldNameFromMethod(method, "get");
					JavaType returnType = objectMapper.constructType(method.getGenericReturnType());
					getters.add(new Getter(method, fieldName, returnType));
				} else if (isBooleanGetter(method)) {
					String fieldName = getFieldNameFromMethod(method, "is");
					JavaType returnType = objectMapper.constructType(method.getGenericReturnType());
					getters.add(new Getter(method, fieldName, returnType));
				} else if (isSetter(method)) {
                    setterFieldsMap.put(method, getSetterFieldNames(method));
				} else if (isClearer(method)) {
					String fieldName = getFieldNameFromMethod(method, "clear");
					clearerFieldsMap.put(method, new String[] {fieldName});
				} else {
					Method passthrough = getPassthroughMethod(method);
					passthroughMethodsMap.put(method, passthrough);
				}
			}
		}

		/* 
		* Methods inherited from Object aren't included in proxyClass.getMethods() but it is necessary to override
		* the default behavior inherited from Object on several methods to ensure proper function
		*/
		Method equalsMethod = getObjectMethod("equals", Object.class);
		passthroughMethodsMap.put(equalsMethod, getPassthroughMethod(equalsMethod));
		
		Method hashCodeMethod = getObjectMethod("hashCode");
		passthroughMethodsMap.put(hashCodeMethod, getPassthroughMethod(hashCodeMethod));

		Method toStringMethod = getObjectMethod("toString");
		passthroughMethodsMap.put(toStringMethod, getPassthroughMethod(toStringMethod));
		
		// If default method implementations are provided, a generated instance of MethodHandles.Lookup is required
		MethodHandles.Lookup lookup = defaultMethods.isEmpty() ? null : generateLookup(proxyClass, lookupGenerator);
		
		// Return constructed instance of ProxyMetadata
		return new ProxyMetadata(proxyClass, daoGetter, defaultMethods, getters, setterFieldsMap, clearerFieldsMap, passthroughMethodsMap, lookup);
	}
	
	private boolean isStatic(Method method) {
		return Modifier.isStatic(method.getModifiers());
	}
	
	private boolean isLookupGenerator(Method method) {
		return MethodHandles.Lookup.class.equals(method.getReturnType())
				&& method.getParameterCount() == 0;
	}
	
	private boolean isSetKeyGetter(Method method) {
		return "getSetKey".equals(method.getName()) && String.class.equals(method.getReturnType()) && method.getParameterCount() == 0;
	}
	
	private boolean isIdGetter(Method method) {
		return "getId".equals(method.getName()) && String.class.equals(method.getReturnType()) && method.getParameterCount() == 0;
	}
	
	private boolean isDaoGetter(Method method) {
		return "getDao".equals(method.getName()) && Dao.class.equals(method.getReturnType()) && method.getParameterCount() == 0;
	}
	
	private boolean isGetter(Method method) {
		return method.getName().startsWith("get") && method.getParameterCount() == 0;
	}
	
	private boolean isBooleanGetter(Method method) {
		return method.getName().startsWith("is") && method.getParameterCount() == 0;
	}
	
	private boolean isSetter(Method method) {
		return method.getName().startsWith("set") && method.getParameterCount() > 0 &&
				(method.getReturnType() == Void.TYPE || hasOptionalSetResultReturnType(method));
	}
	
	private boolean isClearer(Method method) {
		return method.getName().startsWith("clear") && method.getParameterCount() == 0 && 
				(method.getReturnType() == Void.TYPE || hasOptionalSetResultReturnType(method));
	}
	
	private boolean hasOptionalSetResultReturnType(Method method) {
		Type type = method.getGenericReturnType();
		if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) method.getGenericReturnType();
            Type rawType = parameterizedType.getRawType();
            Type[] typeArgs = parameterizedType.getActualTypeArguments();
            return typeArgs.length == 1 && verifyTypeIsClass(rawType, Optional.class) &&
            		verifyTypeIsClass(typeArgs[0], Modification.class);
		} else {
			return false;
		}
	}
	
	private boolean verifyTypeIsClass(Type type, Class<?> expected) {
		if (type instanceof Class) {
			Class<?> clazz = (Class<?>) type;
			return expected.equals(clazz);
		}
		return false;
	}
	
	private String getFieldNameFromMethod(Method method, String prefix) {
		String fieldName = Introspector.decapitalize(method.getName().substring(prefix.length()));
		return isInternal(method) ? "_" + fieldName : fieldName;
	}
	
	private Method getObjectMethod(String methodName, Class<?>... parameterTypes) {
		try {
			return Object.class.getMethod(methodName, parameterTypes);
		} catch (NoSuchMethodException | SecurityException e) {
			throw new ProxyException("Unable to find proxy method " + methodName + " on " + Object.class.getName());
		}
	}
	
	private Method getPassthroughMethod(Method proxyMethod) {
		try {
			return Dao.class.getMethod(proxyMethod.getName(), proxyMethod.getParameterTypes());
		} catch (NoSuchMethodException | SecurityException e) {
			throw new ProxyException("Unable to find passthrough method " + proxyMethod.getDeclaringClass().getName() + "." + proxyMethod.getName() + " on " + Dao.class.getName());
		}
	}
	
	private String[] getSetterFieldNames(Method method) {
        return Stream.of(method.getParameters())
                .map(p -> isInternal(method, p) ? "_" + p.getName() : p.getName())
                .collect(Collectors.toList()).toArray(new String[0]);
	}
	
	private boolean isInternal(Method method) {
		return method.getAnnotation(Internal.class) != null;
	}
	
	private boolean isInternal(Method method, Parameter param) {
		return isInternal(method) || param.getAnnotation(Internal.class) != null;
	}
	
	private MethodHandles.Lookup generateLookup(Class<? extends Proxy> proxyClass, Method lookupGenerator) {
		if (lookupGenerator != null) {
			try {
				return (Lookup) lookupGenerator.invoke(null);
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new ProxyException("Unable to initialize proxy " + proxyClass.getName() + ". Unable to invoke " + lookupGenerator.getName(), e);
			}
		} else {
			throw new ProxyException("Unable to initialize proxy " + proxyClass.getName() + ". Default method implementations were provided without a static method that returns an instance of " + MethodHandles.Lookup.class.getName());
		}
		
	}

}
