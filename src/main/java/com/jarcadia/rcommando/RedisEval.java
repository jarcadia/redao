package com.jarcadia.rcommando;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;

import io.lettuce.core.ScriptOutputType;

public class RedisEval {
    
    private final RedisCommando rcommando;
    private final RedisValueFormatter formatter;
    private String script;
    private String scriptFile;
    private final List<String> keys;
    private final List<String> args;
    private final String[] arrayRef = new String[0];
    private final TypeReference<List<String>> listTypeRef;
    
    protected RedisEval(RedisCommando rcommando, RedisValueFormatter formatter) {
        this.rcommando = rcommando;
        this.formatter = formatter;
        this.keys = new ArrayList<>();
        this.args = new ArrayList<>();
        this.listTypeRef = new TypeReference<List<String>>() {};
    }

    public RedisEval useScriptFile(String scriptFile) {
        this.scriptFile = scriptFile;
        return this;
    }

    public RedisEval setScript(String script) {
        this.script = script;
        return this;
    }
    
    public RedisEval appendScript(String script) {
        this.script = this.script == null ? script : this.script + script;
        return this;
    }
    
    public RedisEval addKey(String key) {
        keys.add(key);
        return this;
    }
    
    public RedisEval addKeys(String... keys) {
        for (String key : keys) {
            this.keys.add(key);
        }
        return this;
    }
    
    public RedisEval addKeys(List<String> keys) {
        for (String key : keys) {
            this.keys.add(key);
        }
        return this;
    }
    
    public RedisEval deserializeAndAddKeys(String serializedKeys) {
        keys.addAll(formatter.deserialize(serializedKeys, listTypeRef));
        return this;
    }
    
    
    public RedisEval addArg(String arg) {
        this.args.add(arg);
        return this;
    }
    
    public RedisEval addArg(double arg) {
        this.args.add(String.valueOf(arg));
        return this;
    }
    
    public RedisEval addArg(int arg) {
        this.args.add(String.valueOf(arg));
        return this;
    }
    
    public RedisEval addArg(long arg) {
        this.args.add(String.valueOf(arg));
        return this;
    }
    
    public RedisEval addArgs(String... args) {
        for (String arg : args) {
            this.args.add(arg);
        }
        return this;
    }
    
    public RedisEval addArgs(Collection<String> args) {
        this.args.addAll(args);
        return this;
    }
    
    public RedisEval addArg(Object toSerialize) {
        this.args.add(formatter.serialize(toSerialize));
        return this;
    }
    
    public int getLastKeyIndex() {
        return this.keys.size();
    }
    
    public int getLastArgIndex() {
        return this.args.size();
    }
    
    public String returnStatus() {
        return execute(ScriptOutputType.STATUS);
    }
    
    public String returnValue() {
        return execute(ScriptOutputType.VALUE);
    }
    
    public int returnInt() {
        Long value = execute(ScriptOutputType.INTEGER);
        return value.intValue();
    }
    
    public long returnLong() {
        return execute(ScriptOutputType.INTEGER);
    }
    
    public Integer returnNullableInt() {
        Long value = execute(ScriptOutputType.INTEGER);
        return value == null ? null : value.intValue();
    }
        
    public List<String> returnMulti() {
        return execute(ScriptOutputType.MULTI);
    }
    
    public boolean returnBoolean() {
        return execute(ScriptOutputType.BOOLEAN);
    }
    
    private <T> T execute(ScriptOutputType outputType) {
        String digest = script != null ?
                rcommando.getScriptDigest(script) :
                rcommando.getScriptFileDigest(scriptFile);
        return rcommando.core().evalsha(digest, outputType, keys(), args());
    }
    
    protected String[] keys() {
        return this.keys.toArray(arrayRef);
    }
    
    protected String[] args() {
        return this.args.toArray(arrayRef);
    }
}
