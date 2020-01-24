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

import io.lettuce.core.RedisClient;

public class RedisCommandoUnitTest {

    static RedisClient redisClient;
    static RedisCommando rcommando;
    static RcSet objs;

    @BeforeAll
    public static void setup() {
        redisClient = RedisClient.create("redis://localhost/15");
        rcommando = new RedisCommando(redisClient);
        objs = rcommando.getSetOf("objs");
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
        Assertions.assertTrue(objs.get("obj").checkedTouch());
        Assertions.assertFalse(objs.get("obj").checkedTouch());
    }

    @Test
    void lotsOfObjectIds() {
        rcommando.core().del("objs");
        IntStream.range(0, 1000).forEach(b -> objs.get(String.valueOf(b)).checkedTouch());
        Assertions.assertEquals(499500, objs.stream().mapToInt(s -> Integer.parseInt(s.getId())).sum(), "All object IDs are accounted for");
        rcommando.core().del("objs");
    }

    @Test
    void addingAndRemovingObjectIds() {
        rcommando.core().del("objs");
        objs.get("a").checkedTouch();
        objs.get("b").checkedTouch();
        objs.get("c").checkedTouch();

        List<String> list = objs.stream().map(obj -> obj.getId()).sorted().collect(Collectors.toList());
        Assertions.assertIterableEquals(Arrays.asList("a", "b", "c"), list);
        rcommando.core().del("objs");
    }

    @Test
    void testStringProperty() {
        RcObject obj = objs.get("a");
        obj.checkedSet("name", "Alpha");
        Assertions.assertEquals("a", objs.stream().findFirst().get().getId());
        Assertions.assertEquals("Alpha", obj.get("name").asString());
    }

    @Test
    void testIntProperty() {
        RcObject obj = objs.get("a");
        obj.checkedSet("age", 23);
        Assertions.assertEquals(23, obj.get("age").asInt(), "Expected value is an integer");
    }

    enum TestEnum {
        HELLO, WORLD;
    }

    @Test
    void testEnumProperty() {
        RcObject obj = objs.get("a");
        obj.checkedSet("greeting", TestEnum.HELLO);
        Assertions.assertEquals(TestEnum.HELLO, obj.get("greeting").as(TestEnum.class));
    }

    @Test
    void testListOfPrimitivesProperty() {
        List<String> values = Arrays.asList("hello", "world");
        RcObject obj = objs.get("a");
        obj.checkedSet("values", values);
        List<String> readBack = obj.get("values").asListOf(String.class);
        Assertions.assertIterableEquals(values, readBack);
    }

    @Test
    void testListOfPojos() {
        List<PersonPojo> values = Arrays.asList(new PersonPojo("John Doe", 30), new PersonPojo("Jane Doe", 35));
        RcObject obj = objs.get("a");
        obj.checkedSet("values", values);
        List<PersonPojo> readBack = obj.get("values").asListOf(PersonPojo.class);
        Assertions.assertIterableEquals(values, readBack);
    }

    @Test
    void testPubSub() throws InterruptedException, ExecutionException {
        final AtomicReference<String> ref = new AtomicReference<>();
        RcSubscription subscription = rcommando.subscribe("test-channel", (channel, val) -> ref.set(val));
        rcommando.core().publish("test-channel", "hello world");
        subscription.close();
        Assertions.assertEquals("hello world", ref.get());
    }
    
    @Test
    void checkedInitWorks() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RcSubscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));
        Assertions.assertTrue(objs.get("a").checkedTouch());
        Thread.sleep(10);
        Assertions.assertEquals("{\"a\":{\"v\":1}}", change.get());
    }

    @Test
    void checkedSetOnExistingObject() throws InterruptedException, ExecutionException {

        RcObject obj = objs.get("a");
        obj.checkedSet("val", 42);
        Assertions.assertEquals(42, obj.get("val").asInt());

        final AtomicReference<String> change = new AtomicReference<>();
        RcSubscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));

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
        RcSubscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));

        RcObject obj = objs.get("a");
        Optional<CheckedSetSingleFieldResult> changes = obj.checkedSet("val", 100);
        Assertions.assertEquals(1, obj.getVersion());
        Assertions.assertEquals(100, obj.get("val").asInt());
        Assertions.assertTrue(changes.isPresent());
        Assertions.assertEquals("val", changes.get().getField());
        Assertions.assertFalse(changes.get().getBefore().isPresent());
        Assertions.assertEquals("100", changes.get().getAfter().asString());

        subscription.close();
        Assertions.assertEquals("{\"a\":{\"v\":1,\"val\":100}}", change.get());
    }

    @Test
    void checkedSetOnEnum() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RcSubscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));

        RcObject obj = objs.get("a");
        obj.checkedSet("val", TestEnum.HELLO);
        Assertions.assertEquals(1, obj.getVersion());
        Assertions.assertEquals(TestEnum.HELLO, obj.get("val").as(TestEnum.class));

        subscription.close();
        Assertions.assertEquals("{\"a\":{\"v\":1,\"val\":\"HELLO\"}}", change.get());
    }

    @Test
    void checkedSetOnMultiLineString() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RcSubscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));

        RcObject obj = objs.get("a");
        Optional<CheckedSetSingleFieldResult> changes = obj.checkedSet("val", "Hello\nWorld");
        Assertions.assertEquals(1, obj.getVersion());
        Assertions.assertEquals("Hello\nWorld", obj.get("val").asString());
        Assertions.assertTrue(changes.isPresent());
        Assertions.assertEquals("val", changes.get().getField());
        Assertions.assertFalse(changes.get().getBefore().isPresent());
        Assertions.assertEquals("Hello\nWorld", changes.get().getAfter().asString());

        subscription.close();
        Assertions.assertEquals("{\"a\":{\"v\":1,\"val\":\"Hello\\nWorld\"}}", change.get());
    }

    @Test
    void checkedSetMultipleValues() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RcSubscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));

        RcObject obj = objs.get("a");
        Optional<CheckedSetMultiFieldResult> changes = obj.checkedSet("str", "hello", "int", 42);
        Assertions.assertEquals(1, obj.getVersion());
        Assertions.assertEquals("hello", obj.get("str").asString());
        Assertions.assertEquals(42, obj.get("int").asInt());
        Assertions.assertEquals(2, changes.get().getChanges().size());

        Assertions.assertEquals("str", changes.get().getChanges().get(0).getField());
        Assertions.assertFalse(changes.get().getChanges().get(0).getBefore().isPresent());
        Assertions.assertEquals("hello", changes.get().getChanges().get(0).getAfter().asString());

        Assertions.assertEquals("int", changes.get().getChanges().get(1).getField());
        Assertions.assertFalse(changes.get().getChanges().get(1).getBefore().isPresent());
        Assertions.assertEquals(42, changes.get().getChanges().get(1).getAfter().asInt());

        subscription.close();
        Assertions.assertEquals("{\"a\":{\"str\":\"hello\",\"int\":42,\"v\":1}}", change.get());
    }

    @Test
    void checkedDeleteToEmpty() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RcSubscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));
        
        RcObject obj = objs.get("a");
        obj.checkedTouch();
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
        RcSubscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));
        
        objs.get("a").checkedTouch();

        RcObject obj = objs.get("b");
        obj.checkedTouch();
        boolean deleted = obj.checkedDelete();
        Assertions.assertTrue(deleted);

        subscription.close();
        Assertions.assertEquals("{\"b\":null}", change.get());
        Assertions.assertEquals(1, objs.size());
        Assertions.assertEquals("a", objs.stream().findFirst().get().getId());
    }

    @Test
    void checkedDeleteNonExist() throws InterruptedException, ExecutionException {
        objs.get("a").checkedTouch();

        final AtomicReference<String> change = new AtomicReference<>();
        RcSubscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));
        
        boolean deleted = objs.get("b").checkedDelete();
        Assertions.assertFalse(deleted);

        subscription.close();
        Assertions.assertNull(change.get());
        
        Assertions.assertEquals(1, objs.size());
        Assertions.assertEquals("a", objs.stream().findFirst().get().getId());
    }

    @Test
    void getMultipleProperties() {
        RcObject obj = objs.get("a");
        obj.set("name", "John Doe");
        obj.set("age", 23);
        
        RcValues values = objs.get("a").get("name", "age");
        Assertions.assertEquals("John Doe", values.next().asString());
        Assertions.assertEquals(23, values.next().asInt());
    }
    
    @Test
    void testClearOnExistingField() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RcSubscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));
        
        RcObject obj = objs.get("a");
        obj.checkedSet("name", "John Doe");
        obj.checkedSet("age", 23);
        Optional<CheckedSetSingleFieldResult> result = obj.checkedClear("age");
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals("age", result.get().getField());
        Assertions.assertEquals(23, result.get().getBefore().asInt());
        Assertions.assertFalse(result.get().getAfter().isPresent());
        subscription.close();
        
        Assertions.assertFalse(obj.get("age").isPresent());
        Assertions.assertEquals("{\"a\":{\"v\":3,\"age\":null}}", change.get());
    }
    
    @Test
    void testClearOnNonExistentField() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        RcSubscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));
        
        RcObject obj = objs.get("a");
        obj.set("name", "John Doe");
        Optional<CheckedSetSingleFieldResult> result = obj.checkedClear("field");
        Assertions.assertFalse(result.isPresent());
        subscription.close();
        Assertions.assertNull(change.get());
    }

    @Test
    void testCdlWithSingleValue() {
        RcCountDownLatch cdl = rcommando.getCdl("abc");
        cdl.init(3, "name", "John Doe");

        Optional<RcValue> result;

        result = cdl.decrement("name");
        Assertions.assertFalse(result.isPresent());

        result = cdl.decrement("name");
        Assertions.assertFalse(result.isPresent());

        result = cdl.decrement("name");
        Assertions.assertTrue(result.isPresent());

        RcValue value = result.get();
        Assertions.assertNotNull(result.get());
        Assertions.assertEquals("John Doe", value.asString());
    }

    @Test
    void testCdlWithMultipleValues() {
        RcCountDownLatch cdl = rcommando.getCdl("abc");
        cdl.init(3, "name", "John Doe", "age", 23);
        
        Optional<RcValues> result;
        
        result = cdl.decrement("name", "age");
        Assertions.assertFalse(result.isPresent());

        result = cdl.decrement("name", "age");
        Assertions.assertFalse(result.isPresent());

        result = cdl.decrement("name", "age");
        Assertions.assertTrue(result.isPresent());

        RcValues values = result.get();
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
