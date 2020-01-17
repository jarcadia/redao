//package com.jarcadia.rcommando;
//
//import java.beans.BeanInfo;
//import java.beans.IntrospectionException;
//import java.beans.Introspector;
//import java.beans.PropertyDescriptor;
//import java.lang.reflect.InvocationHandler;
//import java.lang.reflect.Method;
//import java.util.HashMap;
//import java.util.Map;
//
//public class RedisObjectInvocationHandler implements InvocationHandler {
//    
//    private final RedisObject redisObject;
//    private final Map<Method, Field> getters;
//    
//    public RedisObjectInvocationHandler(RedisObject object, Class<?> targetClass) throws IntrospectionException {
//        this.redisObject = object;
//        this.getters = new HashMap<>();
//        BeanInfo info = Introspector.getBeanInfo(targetClass);
//        
//        for (PropertyDescriptor pd : info.getPropertyDescriptors()) {
//            if (pd.getReadMethod() != null) {
//                getters.put(pd.getReadMethod(), new Field(pd.getName(), pd.getPropertyType()));
//            }
//        }
//    }
//
//    @Override
//    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
//        if (getters.containsKey(method)) {
//            Field field = getters.get(method);
//            System.out.println("Its a getter for " + field.getName() + " " + field.getType().getSimpleName());
//            return redisObject.get(field.getName()).as(field.getType());
//        } else {
//            return null;
//        }
//    }
//
//    private class Field {
//        private final String name;
//        private final Class<?> type;
//
//        public Field(String name, Class<?> type) {
//            this.name = name;
//            this.type = type;
//        }
//
//        public String getName() {
//            return name;
//        }
//
//        public Class<?> getType() {
//            return type;
//        }
//    }
//}
