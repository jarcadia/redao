package com.jarcadia.rcommando;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.lettuce.core.RedisClient;

public class RedisCommandoUnitTest {

    static RedisClient redisClient;
    static ObjectMapper objectMapper;
    static RedisValueFormatter formatter;
    static RedisCommando rcommando;
    static RedisMap objs;

    @BeforeAll
    public static void setup() {
        redisClient = RedisClient.create("redis://localhost/15");
        objectMapper = new ObjectMapper();
        formatter = new RedisValueFormatter(objectMapper);
        rcommando = new RedisCommando(redisClient, formatter);
        objs = rcommando.mapOf("objs");
    }

    @BeforeEach
    public void flush() {
        rcommando.core().flushdb();
    }

    @Test
    void basicKeyValue() {
        rcommando.core().set("hello", "world");
        Assertions.assertEquals("world", rcommando.core().get("hello"), "Key value is read correctly");
        rcommando.core().del("hello");
    }

    @Test
    void lotsOfObjectIds() {
        rcommando.core().del("objs");
        IntStream.range(0, 1000).forEach(b -> objs.get(String.valueOf(b)).touch());
        Assertions.assertEquals(499500, objs.stream().mapToInt(s -> Integer.parseInt(s.getId())).sum(), "All object IDs are accounted for");
        rcommando.core().del("objs");
    }

    @Test
    void addingAndRemovingObjectIds() {
        rcommando.core().del("objs");
        objs.get("a").touch();
        objs.get("b").touch();
        objs.get("c").touch();

        List<String> list = objs.stream().map(obj -> obj.getId()).sorted().collect(Collectors.toList());
        Assertions.assertIterableEquals(Arrays.asList("a", "b", "c"), list);
        rcommando.core().del("objs");
    }

    @Test
    void testStringProperty() {
        RedisObject obj = objs.get("a");
        obj.set("name", "Alpha");
        Assertions.assertEquals("a", objs.stream().findFirst().get().getId());
        Assertions.assertEquals("Alpha", obj.get("name").asString());
    }

    @Test
    void testIntProperty() {
        RedisObject obj = objs.get("a");
        obj.set("age", 23);
        Assertions.assertEquals(23, obj.get("age").asInt(), "Expected value is an integer");
    }
    
    enum TestEnum {
        HELLO, WORLD;
    }
    
    @Test
    void testEnumProperty() {
        RedisObject obj = objs.get("a");
        obj.set("greeting", TestEnum.HELLO);
        Assertions.assertEquals("HELLO", obj.get("greeting").asString());
    }

    @Test
    void testListOfPrimitivesProperty() {
        List<String> values = Arrays.asList("hello", "world");
        RedisObject obj = objs.get("a");
        obj.set("values", values);
        List<String> readBack = obj.get("values").asListOf(String.class);
        Assertions.assertIterableEquals(values, readBack);
    }

    @Test
    void testListOfPojos() {
        List<PersonPojo> values = Arrays.asList(new PersonPojo("John Doe", 30), new PersonPojo("Jane Doe", 35));
        RedisObject obj = objs.get("a");
        obj.set("values", values);
        List<PersonPojo> readBack = obj.get("values").asListOf(PersonPojo.class);
        Assertions.assertIterableEquals(values, readBack);
    }
    
    @Test
    void testPubSub() throws InterruptedException, ExecutionException {
        final AtomicReference<String> ref = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("test-channel", val -> ref.set(val.asString()));
        rcommando.core().publish("test-channel", "hello world");
        subscription.unsubscribe();
        Assertions.assertEquals("hello world", ref.get());
    }
    
    @Test
    void checkedSetOnExistingObject() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("objs.change", val -> change.set(val.asString()));

        RedisObject obj = objs.get("a");
        obj.set("val", 42);
        Assertions.assertEquals(42, obj.get("val").asInt());
        List<RedisChangedValue> changes =  obj.checkedSet("val", 100);
        Assertions.assertEquals(2, obj.getVersion());
        Assertions.assertEquals(100, obj.get("val").asInt());
        Assertions.assertEquals(1, changes.size());
        Assertions.assertEquals("val", changes.get(0).getField());
        Assertions.assertEquals(42, changes.get(0).getBefore().asInt());
        Assertions.assertEquals(100, changes.get(0).getAfter().asInt());

        subscription.unsubscribe();
        Assertions.assertEquals("{\"a\":{\"v\":2,\"val\":\"100\"}}", change.get());
    }

    
    @Test
    void checkedSetOnNewObject() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("objs.change", val -> change.set(val.asString()));

        RedisObject obj = objs.get("a");
        obj.checkedSet("val", 100);
        Assertions.assertEquals(1, obj.getVersion());
        Assertions.assertEquals(100, obj.get("val").asInt());

        subscription.unsubscribe();
        Assertions.assertEquals("{\"a\":{\"v\":1,\"val\":\"100\"}}", change.get());
    }
    
    @Test
    void checkedDeleteToEmpty() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("objs.change", val -> change.set(val.asString()));
        
        RedisObject obj = objs.get("a");
        obj.touch();
        int deleted = obj.checkedDelete();
        Assertions.assertEquals(1, deleted);

        subscription.unsubscribe();
        Assertions.assertEquals("{\"a\":null}", change.get());
        
        Assertions.assertEquals(0, objs.size());
        Assertions.assertEquals(0, objs.stream().count());
    }
    
    @Test
    void checkedDeleteWithRemaining() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("objs.change", val -> change.set(val.asString()));
        
        objs.get("a").touch();

        RedisObject obj = objs.get("b");
        obj.touch();
        int deleted = obj.checkedDelete();
        Assertions.assertEquals(1, deleted);

        subscription.unsubscribe();
        Assertions.assertEquals("{\"b\":null}", change.get());
        Assertions.assertEquals(1, objs.size());
        Assertions.assertEquals("a", objs.stream().findFirst().get().getId());
    }
    
    
    @Test
    void checkedDeleteNonExist() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("objs.change", val -> change.set(val.asString()));
        
        objs.get("a").touch();
        int deleted = objs.get("b").checkedDelete();
        Assertions.assertEquals(0, deleted);

        subscription.unsubscribe();
        Assertions.assertNull(change.get());
        
        Assertions.assertEquals(1, objs.size());
        Assertions.assertEquals("a", objs.stream().findFirst().get().getId());
    }

    @Test
    void getMultipleProperties() {
        RedisObject obj = objs.get("a");
        obj.set("name", "John Doe");
        obj.set("age", 23);
        
        RedisValues values = objs.get("a").get("name", "age");
        Assertions.assertEquals("John Doe", values.get("name").asString());
        Assertions.assertEquals(23, values.get("age").asInt());
    }
    
    @Test
    void testCdlWithSingleValue() {
        RedisCdl cdl = rcommando.getCdl("abc");
        cdl.init(3, "name", "John Doe");
        
        Optional<RedisValue> result;
        
        result = cdl.decrement("name");
        Assertions.assertFalse(result.isPresent());

        result = cdl.decrement("name");
        Assertions.assertFalse(result.isPresent());

        result = cdl.decrement("name");
        Assertions.assertTrue(result.isPresent());

        RedisValue value = result.get();
        Assertions.assertNotNull(result.get());
        Assertions.assertEquals("John Doe", value.asString());
    }
    
    @Test
    void testCdlWithMultipleValues() {
        RedisCdl cdl = rcommando.getCdl("abc");
        cdl.init(3, "name", "John Doe", "age", 23);
        
        Optional<RedisValues> result;
        
        result = cdl.decrement("name", "age");
        Assertions.assertFalse(result.isPresent());

        result = cdl.decrement("name", "age");
        Assertions.assertFalse(result.isPresent());

        result = cdl.decrement("name", "age");
        Assertions.assertTrue(result.isPresent());

        RedisValues values = result.get();
        Assertions.assertNotNull(result.get());
        Assertions.assertEquals("John Doe", values.get("name").asString());
        Assertions.assertEquals(23, values.get("age").asInt());
    }
}
