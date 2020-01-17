package com.jarcadia.rcommando;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

public class RedisCommando {
    
    private final Logger logger = LoggerFactory.getLogger(RedisCommando.class);
    
    private final RedisClient redis;
    private final RedisValueFormatter formatter;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;
    private final Map<String, String> scriptCache;
    private final Map<String, Set<CheckedInsertHandler>> insertHandlerMap;
    private final Map<String, Set<CheckedDeleteHandler>> deleteHandlerMap;
    private final Map<String, Map<String, Set<CheckedSetUpdateHandler>>> updateHandlerMap;
    
    public static RedisCommando create(RedisClient client, ObjectMapper objectMapper) {
        return new RedisCommando(client, new RedisValueFormatter(objectMapper));
    }

    RedisCommando(RedisClient redis, RedisValueFormatter formatter) {
        this.redis = redis;
        this.formatter = formatter;
        this.connection = redis.connect();
        this.commands = connection.sync();
        this.scriptCache = new ConcurrentHashMap<>();
        this.insertHandlerMap = new ConcurrentHashMap<>();
        this.deleteHandlerMap = new ConcurrentHashMap<>();
        this.updateHandlerMap = new ConcurrentHashMap<>();
    }

    public RedisCommando clone() {
        return new RedisCommando(redis, formatter);
    }

    public RedisCommands<String, String> core() {
        return commands;
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

    public RedisMap getMap(String mapKey) {
        return new RedisMap(this, formatter, mapKey);
    }

    public RedisEval eval() {
        return new RedisEval(this, this.formatter);
    }

    public RedisCdl getCdl(String id) {
        return new RedisCdl(this, formatter, id);
    }

    public RedisSubscription subscribe(String channel, Consumer<String> handler) {
        return new RedisSubscription(redis.connectPubSub(), formatter, channel, handler);
    }

    protected String getScriptDigest(String script) {
        return scriptCache.computeIfAbsent(script, s -> commands.scriptLoad(s));
    }

    public void close() {
        connection.close();
    }

    public void registerCheckedInsertHandler(String mapKey, CheckedInsertHandler handler) {
        this.insertHandlerMap.computeIfAbsent(mapKey, k -> ConcurrentHashMap.newKeySet()).add(handler);
    }

    public void registerCheckedDeleteHandler(String mapKey, CheckedDeleteHandler handler) {
        this.deleteHandlerMap.computeIfAbsent(mapKey, k -> ConcurrentHashMap.newKeySet()).add(handler);
    }

    public void registerCheckedSetUpdateHandler(String mapKey, String fieldName, CheckedSetUpdateHandler handler) {
        Map<String, Set<CheckedSetUpdateHandler>> keyUpdateHandlers = updateHandlerMap.computeIfAbsent(mapKey, k-> new ConcurrentHashMap<>());
        keyUpdateHandlers.computeIfAbsent(fieldName, k -> ConcurrentHashMap.newKeySet()).add(handler);
    }

    protected void invokeCheckedInsertHandlers(String mapKey, String id) {
        Set<CheckedInsertHandler> insertHandlers = insertHandlerMap.get(mapKey);
        if (insertHandlers != null) {
            for (CheckedInsertHandler handler : insertHandlers) {
                handler.onInsert(mapKey, id);
            }
        }
    }
    
    protected void invokeCheckedSetCallbacks(CheckedSetSingleFieldResult result) {
        Set<CheckedInsertHandler> insertHandlers = insertHandlerMap.get(result.getMapKey());

        if (insertHandlers != null && result.getVersion() == 1L) {
            for (CheckedInsertHandler handler : insertHandlers) {
                handler.onInsert(result.getMapKey(), result.getId());
            }
        }

        Map<String, Set<CheckedSetUpdateHandler>> updateHandlersForKeyMap = updateHandlerMap.get(result.getMapKey());
        if (updateHandlersForKeyMap != null) {
            Set<CheckedSetUpdateHandler> updateHandlersForField = updateHandlersForKeyMap.get(result.getField());
            if (updateHandlersForField != null) {
                for (CheckedSetUpdateHandler handler : updateHandlersForField) {
                    handler.onChange(result.getMapKey(), result.getId(), result.getVersion(), result.getField(), result.getBefore(), result.getAfter());
                }
            }
        }
    }

    protected void invokeCheckedSetCallbacks(CheckedSetMultiFieldResult result) {
        Set<CheckedInsertHandler> insertHandlers = insertHandlerMap.get(result.getMapKey());
        Map<String, Set<CheckedSetUpdateHandler>> updateHandlersForKeyMap = updateHandlerMap.get(result.getMapKey());
        
        if (insertHandlers != null && result.getVersion() == 1L) {
            for (CheckedInsertHandler handler : insertHandlers) {
                handler.onInsert(result.getMapKey(), result.getId());
            }
        }

        if (updateHandlersForKeyMap != null) {
            for (RedisChangedValue change : result.getChanges()) {
                Set<CheckedSetUpdateHandler> updateHandlersForField = updateHandlersForKeyMap.get(change.getField());
                if (updateHandlersForField != null) {
                    for (CheckedSetUpdateHandler handler : updateHandlersForField) {
                        handler.onChange(result.getMapKey(), result.getId(), result.getVersion(), change.getField(), change.getBefore(), change.getAfter());
                    }
                }
            }
        }
    }

    protected void invokeCheckedDeleteHandlers(String mapKey, String id) {
        Set<CheckedDeleteHandler> deleteHandlers = deleteHandlerMap.get(mapKey);
        if (deleteHandlers != null) {
            for (CheckedDeleteHandler handler : deleteHandlers) {
                handler.onDelete(mapKey, id);
            }
        }
    }
} 