package com.jarcadia.rcommando;

import java.lang.invoke.MethodHandles;
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

import com.jarcadia.rcommando.proxy.DaoProxy;
import com.jarcadia.rcommando.proxy.Internal;

import io.lettuce.core.RedisClient;

public class RedisCommandoUnitTest {

    static RedisClient redisClient;
    static RedisCommando rcommando;
    static DaoSet objs;

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
        Assertions.assertTrue(objs.get("obj").touch());
        Assertions.assertFalse(objs.get("obj").touch());
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
        Dao obj = objs.get("a");
        obj.set("name", "Alpha");
        Assertions.assertEquals("a", objs.stream().findFirst().get().getId());
        Assertions.assertEquals("Alpha", obj.get("name").asString());
    }

    @Test
    void testIntProperty() {
        Dao obj = objs.get("a");
        obj.set("age", 23);
        Assertions.assertEquals(23, obj.get("age").asInt(), "Expected value is an integer");
    }

    enum TestEnum {
        HELLO, WORLD;
    }

    @Test
    void testEnumProperty() {
        Dao obj = objs.get("a");
        obj.set("greeting", TestEnum.HELLO);
        Assertions.assertEquals(TestEnum.HELLO, obj.get("greeting").as(TestEnum.class));
    }

    @Test
    void testListOfPrimitivesProperty() {
        List<String> values = Arrays.asList("hello", "world");
        Dao obj = objs.get("a");
        obj.set("values", values);
        List<String> readBack = obj.get("values").asListOf(String.class);
        Assertions.assertIterableEquals(values, readBack);
    }

    @Test
    void testListOfPojos() {
        List<PersonPojo> values = Arrays.asList(new PersonPojo("John Doe", 30), new PersonPojo("Jane Doe", 35));
        Dao obj = objs.get("a");
        obj.set("values", values);
        List<PersonPojo> readBack = obj.get("values").asListOf(PersonPojo.class);
        Assertions.assertIterableEquals(values, readBack);
    }

    @Test
    void testPubSub() throws InterruptedException, ExecutionException {
        final AtomicReference<String> ref = new AtomicReference<>();
        Subscription subscription = rcommando.subscribe("test-channel", (channel, val) -> ref.set(val));
        rcommando.core().publish("test-channel", "hello world");
        subscription.close();
        Assertions.assertEquals("hello world", ref.get());
    }
    
    @Test
    void checkedInitWorks() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        Subscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));
        Assertions.assertTrue(objs.get("a").touch());
        Thread.sleep(10);
        Assertions.assertEquals("{\"a\":{\"v\":1}}", change.get());
    }

    @Test
    void checkedSetOnExistingObject() throws InterruptedException, ExecutionException {

        Dao obj = objs.get("a");
        obj.set("val", 42);
        Assertions.assertEquals(42, obj.get("val").asInt());

        final AtomicReference<String> change = new AtomicReference<>();
        Subscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));

        Optional<SetResult> changes =  obj.set("val", "hello");
        Assertions.assertEquals("hello", obj.get("val").asString());
        Assertions.assertTrue(changes.isPresent());
        Assertions.assertEquals("val", changes.get().getChanges().get(0).getField());
        Assertions.assertEquals(42, changes.get().getChanges().get(0).getBefore().asInt());
        Assertions.assertEquals("hello", changes.get().getChanges().get(0).getAfter().asString());
        Assertions.assertEquals("{\"a\":{\"v\":2,\"val\":\"hello\"}}", change.get());
    }

    @Test
    void checkedSetOnNewObject() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        Subscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));

        Dao obj = objs.get("a");
        Optional<SetResult> changes = obj.set("val", 100);
        Assertions.assertEquals(100, obj.get("val").asInt());
        Assertions.assertTrue(changes.isPresent());
        Assertions.assertEquals("val", changes.get().getChanges().get(0).getField());
        Assertions.assertFalse(changes.get().getChanges().get(0).getBefore().isPresent());
        Assertions.assertEquals("100", changes.get().getChanges().get(0).getAfter().asString());

        subscription.close();
        Assertions.assertEquals("{\"a\":{\"v\":1,\"val\":100}}", change.get());
    }

    @Test
    void checkedSetOnEnum() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        Subscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));

        Dao obj = objs.get("a");
        obj.set("val", TestEnum.HELLO);
        Assertions.assertEquals(TestEnum.HELLO, obj.get("val").as(TestEnum.class));

        subscription.close();
        Assertions.assertEquals("{\"a\":{\"v\":1,\"val\":\"HELLO\"}}", change.get());
    }

    @Test
    void checkedSetOnMultiLineString() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        Subscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));

        Dao obj = objs.get("a");
        Optional<SetResult> changes = obj.set("val", "Hello\nWorld");
        Assertions.assertEquals("Hello\nWorld", obj.get("val").asString());
        Assertions.assertTrue(changes.isPresent());
        Assertions.assertEquals("val", changes.get().getChanges().get(0).getField());
        Assertions.assertFalse(changes.get().getChanges().get(0).getBefore().isPresent());
        Assertions.assertEquals("Hello\nWorld", changes.get().getChanges().get(0).getAfter().asString());

        subscription.close();
        Assertions.assertEquals("{\"a\":{\"v\":1,\"val\":\"Hello\\nWorld\"}}", change.get());
    }

    @Test
    void checkedSetMultipleValues() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        Subscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));

        Dao obj = objs.get("a");
        Optional<SetResult> changes = obj.set("str", "hello", "int", 42);
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
    void checkedSetWithInternalFields() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        Subscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));

        Dao obj = objs.get("a");
        Optional<SetResult> changes = obj.set("_str", "hello", "int", 42);
        Assertions.assertEquals("hello", obj.get("_str").asString());
        Assertions.assertEquals(42, obj.get("int").asInt());
        Assertions.assertEquals(2, changes.get().getChanges().size());

        Assertions.assertEquals("_str", changes.get().getChanges().get(0).getField());
        Assertions.assertFalse(changes.get().getChanges().get(0).getBefore().isPresent());
        Assertions.assertEquals("hello", changes.get().getChanges().get(0).getAfter().asString());

        Assertions.assertEquals("int", changes.get().getChanges().get(1).getField());
        Assertions.assertFalse(changes.get().getChanges().get(1).getBefore().isPresent());
        Assertions.assertEquals(42, changes.get().getChanges().get(1).getAfter().asInt());

        subscription.close();
        Assertions.assertEquals("{\"a\":{\"int\":42,\"v\":1}}", change.get());
    }


    @Test
    void checkedDeleteToEmpty() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        Subscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));
        
        Dao obj = objs.get("a");
        obj.touch();
        boolean deleted = obj.delete();
        Assertions.assertTrue(deleted);

        subscription.close();
        Assertions.assertEquals("{\"a\":null}", change.get());
        
        Assertions.assertEquals(0, objs.size());
        Assertions.assertEquals(0, objs.stream().count());
    }

    @Test
    void checkedDeleteWithRemaining() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        Subscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));
        
        objs.get("a").touch();

        Dao obj = objs.get("b");
        obj.touch();
        boolean deleted = obj.delete();
        Assertions.assertTrue(deleted);

        subscription.close();
        Assertions.assertEquals("{\"b\":null}", change.get());
        Assertions.assertEquals(1, objs.size());
        Assertions.assertEquals("a", objs.stream().findFirst().get().getId());
    }

    @Test
    void checkedDeleteNonExist() throws InterruptedException, ExecutionException {
        objs.get("a").touch();

        final AtomicReference<String> change = new AtomicReference<>();
        Subscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));
        
        boolean deleted = objs.get("b").delete();
        Assertions.assertFalse(deleted);

        subscription.close();
        Assertions.assertNull(change.get());
        
        Assertions.assertEquals(1, objs.size());
        Assertions.assertEquals("a", objs.stream().findFirst().get().getId());
    }

    @Test
    void getMultipleProperties() {
        Dao obj = objs.get("a");
        obj.set("name", "John Doe");
        obj.set("age", 23);
        
        DaoValues values = objs.get("a").get("name", "age");
        Assertions.assertEquals("John Doe", values.next().asString());
        Assertions.assertEquals(23, values.next().asInt());
    }
    
    @Test
    void testClearOnExistingField() throws InterruptedException, ExecutionException {
        final AtomicReference<String> change = new AtomicReference<>();
        Subscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));
        
        Dao obj = objs.get("a");
        obj.set("name", "John Doe");
        obj.set("age", 23);
        Optional<SetResult> result = obj.clear("age");
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals("age", result.get().getChanges().get(0).getField());
        Assertions.assertEquals(23, result.get().getChanges().get(0).getBefore().asInt());
        Assertions.assertFalse(result.get().getChanges().get(0).getAfter().isPresent());
        subscription.close();
        
        Assertions.assertFalse(obj.get("age").isPresent());
        Assertions.assertEquals("{\"a\":{\"v\":3,\"age\":null}}", change.get());
    }
    
    @Test
    void testClearOnNonExistentField() throws InterruptedException, ExecutionException {
        
        Dao obj = objs.get("a");
        obj.set("name", "John Doe");

        final AtomicReference<String> change = new AtomicReference<>();
        Subscription subscription = rcommando.subscribe("objs.change", (channel, val) -> change.set(val));

        Optional<SetResult> result = obj.clear("field");
        Assertions.assertFalse(result.isPresent());
        subscription.close();
        Assertions.assertNull(change.get());
    }

    @Test
    void testCdlWithSingleValue() {
        CountDownLatch cdl = rcommando.getCountDownLatch("abc");
        cdl.init(3, "name", "John Doe");

        Optional<DaoValue> result;

        result = cdl.decrement("name");
        Assertions.assertFalse(result.isPresent());

        result = cdl.decrement("name");
        Assertions.assertFalse(result.isPresent());

        result = cdl.decrement("name");
        Assertions.assertTrue(result.isPresent());

        DaoValue value = result.get();
        Assertions.assertNotNull(result.get());
        Assertions.assertEquals("John Doe", value.asString());
    }

    @Test
    void testCdlWithMultipleValues() {
        CountDownLatch cdl = rcommando.getCountDownLatch("abc");
        cdl.init(3, "name", "John Doe", "age", 23);
        
        Optional<DaoValues> result;
        
        result = cdl.decrement("name", "age");
        Assertions.assertFalse(result.isPresent());

        result = cdl.decrement("name", "age");
        Assertions.assertFalse(result.isPresent());

        result = cdl.decrement("name", "age");
        Assertions.assertTrue(result.isPresent());

        DaoValues values = result.get();
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
    
    public interface PersonProxy extends DaoProxy {
    	
    	public static MethodHandles.Lookup createLookup() {
    		return MethodHandles.lookup();
    	}

    	public String getName();
    	public int getAge();
    	public Optional<String> getEmail();
    	public Optional<String> getFax();
    	public List<String> getNicknames();
    	public boolean isEmployed();
    	@Internal public String getSocialSecurityNumber();
    	default public int getSalary() {
    		return 100000;
    	}
    	default public boolean needsRaise(int minimumSalary) {
    		return this.getSalary() >= getSalary();
    	}
    	
    	public void setName(String name);
    	public void setAge(int age);
    	public void setIQ(int iq);
    	@Internal public void setSocialSecurityNumber(String socialSecurityNumber);
    	public void setSomethingComplicated(String name, int age, int IQ, @Internal String socialSecurityNumber);
    }
    
    @Test
    void testProxyGet() {
    	Dao obj = objs.get("abc123");
    	obj.set("name", "John Doe", "age", 23, "email", "jd@test.com", "employed", true, "_socialSecurityNumber",
    			"111-22-3333", "nicknames", List.of("Jdizzle", "Jdog"));
    	PersonProxy proxy = obj.as(PersonProxy.class);
    	Assertions.assertEquals("abc123", proxy.getId());
    	Assertions.assertEquals("John Doe", proxy.getName());
    	Assertions.assertEquals(23, proxy.getAge());
    	Assertions.assertEquals("jd@test.com", proxy.getEmail().get());
    	Assertions.assertTrue(proxy.isEmployed());
    	Assertions.assertTrue(proxy.getFax().isEmpty());
    	Assertions.assertIterableEquals(List.of("Jdizzle", "Jdog"), proxy.getNicknames());
    	Assertions.assertEquals(100000, proxy.getSalary());
    	Assertions.assertTrue(proxy.needsRaise(10500));
    	Assertions.assertEquals(23, proxy.getDao().get("age").asInt());
    	Assertions.assertEquals("111-22-3333", proxy.getSocialSecurityNumber());
    }
    
    @Test
    void testProxySet() {
    	Dao obj = objs.get("abc123");
    	PersonProxy proxy = obj.as(PersonProxy.class);
    	proxy.setName("Jane Doe");
    	proxy.setAge(24);
    	proxy.setIQ(145);
    	proxy.setSocialSecurityNumber("111-22-3333");
    	Assertions.assertEquals("Jane Doe", obj.get("name").asString());
    	Assertions.assertEquals(24, obj.get("age").asInt());
    	Assertions.assertEquals(145, obj.get("iq").asInt());
    	Assertions.assertEquals("111-22-3333", obj.get("_socialSecurityNumber").asString());

    	Assertions.assertEquals("abc123", proxy.getId());
    	Assertions.assertEquals("Jane Doe", proxy.getName());
    	Assertions.assertEquals(24, proxy.getAge());
    	Assertions.assertEquals("111-22-3333", proxy.getSocialSecurityNumber());
    }
    
    @Test
    void testProxyMultiSet() {
    	Dao obj = objs.get("abc123");
    	PersonProxy proxy = obj.as(PersonProxy.class);
    	proxy.setSomethingComplicated("Jane Doe", 24, 145, "111-22-3333");
    	Assertions.assertEquals("Jane Doe", obj.get("name").asString());
    	Assertions.assertEquals(24, obj.get("age").asInt());
    	Assertions.assertEquals(145, obj.get("IQ").asInt());
    	Assertions.assertEquals("111-22-3333", obj.get("_socialSecurityNumber").asString());

    	Assertions.assertEquals("abc123", proxy.getId());
    	Assertions.assertEquals("Jane Doe", proxy.getName());
    	Assertions.assertEquals(24, proxy.getAge());
    	Assertions.assertEquals("111-22-3333", proxy.getSocialSecurityNumber());
    }
    
    @Test
    void testProxyEquals() {
    	PersonProxy p1 = objs.get("abc123").as(PersonProxy.class);
    	PersonProxy p2 = objs.get("abc123").as(PersonProxy.class);
    	Assertions.assertTrue(p1.equals(p2));
    }
}
