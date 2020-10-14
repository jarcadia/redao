package dev.jarcadia.redao;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import dev.jarcadia.redao.ProxyMetadata.Getter;
import dev.jarcadia.redao.exception.ProxyException;
import dev.jarcadia.redao.exception.RedisCommandoException;
import dev.jarcadia.redao.proxy.Proxy;

class ProxyInvocationHandler implements InvocationHandler {
	
	private final Dao dao;
	private final ProxyMetadata metadata;
	private final Map<Method, Object> getterValues;

	protected ProxyInvocationHandler(Dao dao, ProxyMetadata metadata) {
		this.dao = dao;
		this.metadata = metadata;
		this.getterValues = new ConcurrentHashMap<>();
	}
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		switch(metadata.getType(method)) {
		case DEFAULT:
			return invokeDefault(proxy, method, args);
		case GETTER:
			return invokeGetter(method);
		case SETTER:
			return invokeSetter(metadata.getSetterFields(method), args);
		case CLEARER:
			return invokeClearer(metadata.getClearerFields(method));
		case PASSTHROUGH:
			return invokePassthrough(metadata.getObjectMethod(method), args);
		case IMPLEMENTED:
			return invokeImplemented(method, args);
		default:
			throw new RedisCommandoException("Unable to proxy unrecognized method " + method.getName());
		}
	}
	
	private Object invokeDefault(Object proxy, Method method, Object[] args) throws IllegalAccessException, Throwable {
        return metadata.getLookup()
                .unreflectSpecial(method, method.getDeclaringClass())
                .bindTo(proxy)
                .invokeWithArguments(args);
	}

	private Object invokeGetter(Method method) {
        Object value = getterValues.get(method);
        if (value == null) {
        	loadCache();
        	return getterValues.get(method);
        } else {
            return value;
        }
	}
	
	private Object invokeSetter(String[] fields, Object[] args) {
		Map<String, Object> values = new HashMap<>();
		for (int i=0; i<fields.length; i++) {
			values.put(fields[i], args[i]);
            setCacheValue(fields[i], args[i]);
		}
		return dao.setAll(values);
	}

	private Object invokeClearer(String[] fields) {
		for (String field : fields) {
			setCacheValue(field, null);
		}
		return dao.clear(fields);
	}
	
	private Object invokePassthrough(Method objMethod, Object[] args) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        return objMethod.invoke(dao, args);
	}
	
	private Object invokeImplemented(Method method, Object[] args) {
		if (method.equals(metadata.getDaoGetter())) {
			return dao;
		} else if (method.equals(metadata.getReproxyMethod())) {
			// Extract args
		    return dao.as((Class<? extends Proxy>) args[0]);
		} else {
			throw new ProxyException("No implementation defined for proxy-implemented method " + method.getName());
		}
	}
	
	private void loadCache() {
//		System.out.println("Lazy loading values for " + dao.getSetKey() + "." + dao.getId());
    	if (!metadata.getGetters().isEmpty()) {
            Iterator<DaoValue> values = dao.get(metadata.getGetterFieldNames()).iterator();
            for (Getter getter : metadata.getGetters()) {
                DaoValue rcv = values.next();
                Object value = rcv.isPresent() ? rcv.as(getter.getReturnType()) : 
                    Optional.class.equals(getter.getReturnType().getRawClass()) ? Optional.empty() : null;
                if (value == null) {
                	this.getterValues.remove(getter.getMethod());
                } else {
                    this.getterValues.put(getter.getMethod(), value);
                }
            }
    	}
	}
	
	private void setCacheValue(String fieldName, Object value) {
        Method companionGetter = metadata.getGetter(fieldName);
        if (companionGetter != null) {
        	if (value == null) {
        		getterValues.remove(companionGetter);
        	} else {
                getterValues.put(companionGetter, value);
        	}
        }
	}
}
