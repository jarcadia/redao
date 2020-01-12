package com.jarcadia.rcommando;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
        objs = rcommando.getMap("objs");
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
    void basicObjInit() {
        Assertions.assertTrue(objs.get("obj").init());
        Assertions.assertFalse(objs.get("obj").init());
    }

    @Test
    void lotsOfObjectIds() {
        rcommando.core().del("objs");
        IntStream.range(0, 1000).forEach(b -> objs.get(String.valueOf(b)).init());
        Assertions.assertEquals(499500, objs.stream().mapToInt(s -> Integer.parseInt(s.getId())).sum(), "All object IDs are accounted for");
        rcommando.core().del("objs");
    }

    @Test
    void addingAndRemovingObjectIds() {
        rcommando.core().del("objs");
        objs.get("a").init();
        objs.get("b").init();
        objs.get("c").init();

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
        Assertions.assertEquals(TestEnum.HELLO, obj.get("greeting").as(TestEnum.class));
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
        RedisSubscription subscription = rcommando.subscribe("test-channel", val -> ref.set(val));
        rcommando.core().publish("test-channel", "hello world");
        subscription.close();
        Assertions.assertEquals("hello world", ref.get());
    }
    
    @Test
    void checkedInitWorks() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("objs.change", val -> change.set(val));
        Assertions.assertTrue(objs.get("a").checkedInit());
        Thread.sleep(10);
        Assertions.assertEquals("{\"a\":{\"v\":1}}", change.get());
    }

    @Test
    void checkedSetOnExistingObject() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("objs.change", val -> change.set(val));

        RedisObject obj = objs.get("a");
        obj.set("val", 42);
        Assertions.assertEquals(42, obj.get("val").asInt());
        Optional<CheckedSetSingleFieldResult> changes =  obj.checkedSet("val", "hello");
        Assertions.assertEquals(2, obj.getVersion());
        Assertions.assertEquals("hello", obj.get("val").asString());
        Assertions.assertTrue(changes.isPresent());
        Assertions.assertEquals("val", changes.get().getField());
        Assertions.assertEquals(42, changes.get().getBefore().asInt());
        Assertions.assertEquals("hello", changes.get().getAfter().asString());
        Assertions.assertEquals("{\"a\":{\"v\":2,\"val\":\"hello\"}}", change.get());
    }

    @Test
    void checkedSetOnNewObject() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("objs.change", val -> change.set(val));

        RedisObject obj = objs.get("a");
        Optional<CheckedSetSingleFieldResult> changes = obj.checkedSet("val", 100);
        Assertions.assertEquals(1, obj.getVersion());
        Assertions.assertEquals(100, obj.get("val").asInt());
        Assertions.assertTrue(changes.isPresent());
        Assertions.assertEquals("val", changes.get().getField());
        Assertions.assertTrue(changes.get().getBefore().isNull());
        Assertions.assertEquals("100", changes.get().getAfter().asString());

        subscription.close();
        Assertions.assertEquals("{\"a\":{\"v\":1,\"val\":100}}", change.get());
    }

    @Test
    void checkedSetOnEnum() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("objs.change", val -> change.set(val));

        RedisObject obj = objs.get("a");
        obj.checkedSet("val", TestEnum.HELLO);
        Assertions.assertEquals(1, obj.getVersion());
        Assertions.assertEquals(TestEnum.HELLO, obj.get("val").as(TestEnum.class));

        subscription.close();
        Assertions.assertEquals("{\"a\":{\"v\":1,\"val\":\"HELLO\"}}", change.get());
    }

    @Test
    void checkedSetOnMultiLineString() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("objs.change", val -> change.set(val));

        RedisObject obj = objs.get("a");
        Optional<CheckedSetSingleFieldResult> changes = obj.checkedSet("val", "Hello\nWorld");
        Assertions.assertEquals(1, obj.getVersion());
        Assertions.assertEquals("Hello\nWorld", obj.get("val").asString());
        Assertions.assertTrue(changes.isPresent());
        Assertions.assertEquals("val", changes.get().getField());
        Assertions.assertTrue(changes.get().getBefore().isNull());
        Assertions.assertEquals("Hello\nWorld", changes.get().getAfter().asString());

        subscription.close();
        Assertions.assertEquals("{\"a\":{\"v\":1,\"val\":\"Hello\\nWorld\"}}", change.get());
    }

    @Test
    void checkedSetMultipleValues() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("objs.change", val -> change.set(val));

        RedisObject obj = objs.get("a");
        Optional<CheckedSetMultiFieldResult> changes = obj.checkedSet("str", "hello", "int", 42);
        Assertions.assertEquals(1, obj.getVersion());
        Assertions.assertEquals("hello", obj.get("str").asString());
        Assertions.assertEquals(42, obj.get("int").asInt());
        Assertions.assertEquals(2, changes.get().getChanges().size());

        Assertions.assertEquals("str", changes.get().getChanges().get(0).getField());
        Assertions.assertTrue(changes.get().getChanges().get(0).getBefore().isNull());
        Assertions.assertEquals("hello", changes.get().getChanges().get(0).getAfter().asString());

        Assertions.assertEquals("int", changes.get().getChanges().get(1).getField());
        Assertions.assertTrue(changes.get().getChanges().get(1).getBefore().isNull());
        Assertions.assertEquals(42, changes.get().getChanges().get(1).getAfter().asInt());

        subscription.close();
        Assertions.assertEquals("{\"a\":{\"str\":\"hello\",\"int\":42,\"v\":1}}", change.get());
    }

    @Test
    void checkedDeleteToEmpty() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("objs.change", val -> change.set(val));
        
        RedisObject obj = objs.get("a");
        obj.init();
        boolean deleted = obj.checkedDelete();
        Assertions.assertTrue(deleted);

        subscription.close();
        Assertions.assertEquals("{\"a\":null}", change.get());
        
        Assertions.assertEquals(0, objs.size());
        Assertions.assertEquals(0, objs.stream().count());
    }

    @Test
    void checkedDeleteWithRemaining() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("objs.change", val -> change.set(val));
        
        objs.get("a").init();

        RedisObject obj = objs.get("b");
        obj.init();
        boolean deleted = obj.checkedDelete();
        Assertions.assertTrue(deleted);

        subscription.close();
        Assertions.assertEquals("{\"b\":null}", change.get());
        Assertions.assertEquals(1, objs.size());
        Assertions.assertEquals("a", objs.stream().findFirst().get().getId());
    }

    @Test
    void checkedDeleteNonExist() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("objs.change", val -> change.set(val));
        
        objs.get("a").init();
        boolean deleted = objs.get("b").checkedDelete();
        Assertions.assertFalse(deleted);

        subscription.close();
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
        Assertions.assertEquals("John Doe", values.next().asString());
        Assertions.assertEquals(23, values.next().asInt());
    }
    
    @Test
    void testClearOnExistingField() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("objs.change", val -> change.set(val));
        
        RedisObject obj = objs.get("a");
        obj.set("name", "John Doe");
        obj.set("age", 23);
        Optional<CheckedSetSingleFieldResult> result = obj.checkedClear("age");
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals("age", result.get().getField());
        Assertions.assertEquals(23, result.get().getBefore().asInt());
        Assertions.assertTrue(result.get().getAfter().isNull());
        subscription.close();
        
        Assertions.assertTrue(obj.get("age").isNull());
        Assertions.assertEquals("{\"a\":{\"v\":3,\"age\":null}", change.get());
    }
    
    @Test
    void testClearOnNonExistentField() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RedisSubscription subscription = rcommando.subscribe("objs.change", val -> change.set(val));
        
        RedisObject obj = objs.get("a");
        obj.set("name", "John Doe");
        Optional<CheckedSetSingleFieldResult> result = obj.checkedClear("field");
        Assertions.assertFalse(result.isPresent());
        subscription.close();
        Assertions.assertNull(change.get());
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
        Assertions.assertEquals("John Doe", values.next().asString());
        Assertions.assertEquals(23, values.next().asInt());
    }

    @Test
    void testMergeIntoSetIfDistinct() {
        rcommando.core().sadd("test", "a", "b", "c");
        Set<String> duplicates = rcommando.mergeIntoSetIfDistinct("test", Arrays.asList("d", "e"));
        Assertions.assertEquals(0, duplicates.size());
        Assertions.assertIterableEquals(Arrays.asList("a", "b", "c", "d", "e"), rcommando.core().smembers("test").stream().sorted().collect(Collectors.toList()));
    }

    @Test
    void testMergeIntoSetIfDistinctWithDuplicates() {
        rcommando.core().sadd("test", "a", "b", "c", "d");
        Set<String> duplicates = rcommando.mergeIntoSetIfDistinct("test", Arrays.asList("b", "d"));
        Assertions.assertEquals(2, duplicates.size());
        Assertions.assertIterableEquals(Arrays.asList("b", "d"), duplicates.stream().sorted().collect(Collectors.toList()));
        Assertions.assertIterableEquals(Arrays.asList("a", "b", "c", "d"), rcommando.core().smembers("test").stream().sorted().collect(Collectors.toList()));
    }
}
