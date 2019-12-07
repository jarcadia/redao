package com.jarcadia.rcommando;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    
    public RedisCommando(RedisClient redis, RedisValueFormatter formatter) {
        this.redis = redis;
        this.formatter = formatter;
        this.connection = redis.connect();
        this.commands = connection.sync();
        this.scriptCache = new ConcurrentHashMap<>();
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

    public List<String> addSetDifferenceAndReturnDuplicates(String existingSetKey, Collection<String> keys) {
        String setKey = UUID.randomUUID().toString();
        return eval()
              .useScriptFile("addSetDifference")
              .addKeys(existingSetKey, setKey)
              .addArgs(keys)
              .returnMulti();
    }

    public RedisMap mapOf(String objType) {
        return new RedisMap(this, formatter, objType);
    }

    public RedisEval eval() {
        return new RedisEval(this, this.formatter);
    }

    public RedisCdl getCdl(String id) {
        return new RedisCdl(this, formatter, id);
    }

    public RedisSubscription subscribe(String channel, Consumer<RedisValue> handler) {
        return new RedisSubscription(redis.connectPubSub(), formatter, channel, handler);
    }

    protected String getScriptDigest(String script) {
        String digest = scriptCache.get(script);
        if (digest == null) {
            digest = commands.scriptLoad(script);
            scriptCache.put(script, digest);
        }
        return digest;
    }

    protected String getScriptFileDigest(String scriptName) {
        String digest = scriptCache.get(scriptName);
        if (digest == null) {
            try {
                File file = new File(getClass().getClassLoader().getResource("lua/" + scriptName + ".lua").getFile());
                String script = new String(Files.readAllBytes(file.toPath()));
                digest = commands.scriptLoad(script);
                scriptCache.put(script, digest);
            } catch (IOException ex) {
                throw new RedisException("Unable to load lua script from classpath://lua/" + scriptName + ".lua", ex);
            }
        }
        return digest;
    }

    public void close() {
        connection.close();
    }
} 