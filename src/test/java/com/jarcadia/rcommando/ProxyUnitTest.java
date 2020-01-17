//package com.jarcadia.rcommando;
//
//import java.beans.IntrospectionException;
//import java.lang.reflect.Proxy;
//
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//import io.lettuce.core.RedisClient;
//
//public class ProxyUnitTest {
//    
//    static RedisClient redisClient;
//    static ObjectMapper objectMapper;
//    static RedisValueFormatter formatter;
//    static RedisCommando rcommando;
//    static RedisMap objs;
//
//    @BeforeAll
//    public static void setup() {
//        redisClient = RedisClient.create("redis://localhost/15");
//        objectMapper = new ObjectMapper();
//        formatter = new RedisValueFormatter(objectMapper);
//        rcommando = new RedisCommando(redisClient, formatter);
//        objs = rcommando.getMap("objs");
//    }
//
//    @BeforeEach
//    public void flush() {
//        rcommando.core().flushdb();
//    }
//    
//    interface TestPojo {
//        public String getField();
//        
//        public void setField(String value);
//    }
//
//    @Test
//    void basicKeyValue() throws IntrospectionException {
//        RedisObject obj = objs.get("abc");
//        obj.set("field", "value");
//        
//        TestPojo pojo = objs.get("abc", TestPojo.class);
//      
//        System.out.println(pojo.getField());
//
//
////        rcommando.core().set("hello", "world");
////        Assertions.assertEquals("world", rcommando.core().get("hello"), "Key value is read correctly");
////        rcommando.core().del("hello");
//    }
//}
