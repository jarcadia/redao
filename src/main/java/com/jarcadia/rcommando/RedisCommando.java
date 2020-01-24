package com.jarcadia.rcommando;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

public class RedisCommando {
    
    private final Logger logger = LoggerFactory.getLogger(RedisCommando.class);
    
    private final RedisClient redis;
    private final ObjectMapper objectMapper;
    private final RedisValueFormatter formatter;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;
    private final Map<String, String> scriptCache;
    private final Map<String, Set<ObjectInsertCallback>> insertCallbackMap;
    private final Map<String, Set<ObjectDeleteCallback>> deleteCallbackMap;
    private final Map<String, Map<String, Set<FieldChangeCallback>>> changeCallbackMap;
    
    public static RedisCommando create(RedisClient client) {
        return new RedisCommando(client);
    }

    RedisCommando(RedisClient redis) {
        this.redis = redis;
    	this.objectMapper = new RcObjectMapper(this);
    	this.formatter = new RedisValueFormatter(objectMapper);
        this.connection = redis.connect();
        this.commands = connection.sync();
        this.scriptCache = new ConcurrentHashMap<>();
        this.insertCallbackMap = new ConcurrentHashMap<>();
        this.deleteCallbackMap = new ConcurrentHashMap<>();
        this.changeCallbackMap = new ConcurrentHashMap<>();
    }

    public RedisCommando clone() {
        return new RedisCommando(redis);
    }

    public RedisCommands<String, String> core() {
        return commands;
    }
    
    public ObjectMapper getObjectMapper() {
    	return this.objectMapper;
    }

    public Integer hgetset(String hashKey, String field, int value) {
        String old = this.hgetset(hashKey, field, String.valueOf(value));
        return old == null ? null : Integer.parseInt(old);
    }

    public String hgetset(String hashKey, String field, String value) {
        return commands.eval("local old = redis.call('hget',KEYS[1],ARGV[1]); redis.call('hset',KEYS[1],ARGV[1],ARGV[2]); return old;", ScriptOutputType.VALUE, new String[] {hashKey}, field, value);
    }

    public Set<String> mergeIntoSetIfDistinct(String setKey, Collection<String> values) {
        Set<String> result = new HashSet<>();
        String tempSetKey = UUID.randomUUID().toString();
        List<String> duplicates =  eval()
        		.cachedScript(Scripts.MERGE_INTO_SET_IF_DISTINCT)
        		.addKeys(setKey, tempSetKey)
        		.addArgs(values)
        		.returnMulti();
        result.addAll(duplicates);
        return result;
    }

    public RcSet getSetOf(String mapKey) {
        return new RcSet(this, formatter, mapKey);
    }

    public RedisEval eval() {
        return new RedisEval(this, this.formatter);
    }

    public RcCountDownLatch getCdl(String id) {
        return new RcCountDownLatch(this, formatter, id);
    }

    public RcSubscription subscribe(BiConsumer<String, String> handler) {
        return new RcSubscription(redis.connectPubSub(), formatter, handler);
    }

    public RcSubscription subscribe(String channel, BiConsumer<String, String> handler) {
        return new RcSubscription(redis.connectPubSub(), formatter, handler, channel);
    }

    protected <T> T executeScript(String script, ScriptOutputType outputType, String[] keys, String[] args) {
        String digest = scriptCache.computeIfAbsent(script, s -> commands.scriptLoad(s));
        try {
            return commands.evalsha(digest, outputType, keys, args);
        } catch (RedisNoScriptException ex) {
        	scriptCache.remove(script);
        	return executeScript(script, outputType, keys, args);
        }
    }

    public void close() {
        connection.close();
    }

    public void registerObjectInsertCallback(String setKey, ObjectInsertCallback handler) {
        this.insertCallbackMap.computeIfAbsent(setKey, k -> ConcurrentHashMap.newKeySet()).add(handler);
    }

    public void registerObjectDeleteCallback(String setKey, ObjectDeleteCallback handler) {
        this.deleteCallbackMap.computeIfAbsent(setKey, k -> ConcurrentHashMap.newKeySet()).add(handler);
    }

    public void registerFieldChangeCallback(String setKey, String fieldName, FieldChangeCallback handler) {
        Map<String, Set<FieldChangeCallback>> keyUpdateHandlers = changeCallbackMap.computeIfAbsent(setKey, k-> new ConcurrentHashMap<>());
        keyUpdateHandlers.computeIfAbsent(fieldName, k -> ConcurrentHashMap.newKeySet()).add(handler);
    }

    protected void invokeObjectInsertCallbacks(String setKey, String id) {
        Set<ObjectInsertCallback> insertCallbacks = insertCallbackMap.get(setKey);
        if (insertCallbacks != null) {
            for (ObjectInsertCallback callback : insertCallbacks) {
                callback.onInsert(setKey, id);
            }
        }
    }
    
    protected void invokeChangeCallbacks(CheckedSetSingleFieldResult result) {
        Set<ObjectInsertCallback> insertCallbacks = insertCallbackMap.get(result.getSetKey());

        if (insertCallbacks != null && result.getVersion() == 1L) {
            for (ObjectInsertCallback callback : insertCallbacks) {
                callback.onInsert(result.getSetKey(), result.getId());
            }
        }

        Map<String, Set<FieldChangeCallback>> changeCallbacksForSet = changeCallbackMap.get(result.getSetKey());
        if (changeCallbacksForSet != null) {
            Set<FieldChangeCallback> changeCallbacks = changeCallbacksForSet.get(result.getField());
            if (changeCallbacks != null) {
            	logger.trace("Invoking {} change callbacks for {}.{}", changeCallbacks.size(), result.getSetKey(), result.getField());
                for (FieldChangeCallback callback : changeCallbacks) {
                    callback.onChange(result.getSetKey(), result.getId(), result.getVersion(), result.getField(), result.getBefore(), result.getAfter());
                }
            }
        }
    }

    protected void invokeChangeCallbacks(CheckedSetMultiFieldResult result) {
        Set<ObjectInsertCallback> insertCallbacks = insertCallbackMap.get(result.getSetKey());
        Map<String, Set<FieldChangeCallback>> changeCallbacksForSet = changeCallbackMap.get(result.getSetKey());
        
        if (insertCallbacks != null && result.getVersion() == 1L) {
            for (ObjectInsertCallback callback : insertCallbacks) {
                callback.onInsert(result.getSetKey(), result.getId());
            }
        }

        if (changeCallbacksForSet != null) {
            for (RedisChangedValue change : result.getChanges()) {
                Set<FieldChangeCallback> changeCallbacks = changeCallbacksForSet.get(change.getField());
                if (changeCallbacks != null) {
                    logger.trace("Invoking {} change callbacks for {}.{}", changeCallbacks.size(), result.getSetKey(), change.getField());
                    for (FieldChangeCallback callback : changeCallbacks) {
                        callback.onChange(result.getSetKey(), result.getId(), result.getVersion(), change.getField(), change.getBefore(), change.getAfter());
                    }
                }
            }
        }
    }

    protected void invokeDeleteCallbacks(String setKey, String id) {
        Set<ObjectDeleteCallback> deleteCallbacks = deleteCallbackMap.get(setKey);
        if (deleteCallbacks != null) {
            for (ObjectDeleteCallback callback : deleteCallbacks) {
                callback.onDelete(setKey, id);
            }
        }
    }
} 