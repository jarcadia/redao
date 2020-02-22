package com.jarcadia.rcommando;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.jarcadia.rcommando.exception.RcDeserializationException;
import com.jarcadia.rcommando.exception.RedisCommandoException;

import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;

public class Eval {
    
    private final RedisCommando rcommando;
    private final ValueFormatter formatter;
    private String script;
    private final List<String> keys;
    private final List<String> args;
    private final String[] arrayRef = new String[0];
    private final TypeReference<List<String>> listTypeRef;
    
    protected Eval(RedisCommando rcommando, ValueFormatter formatter) {
        this.rcommando = rcommando;
        this.formatter = formatter;
        this.keys = new ArrayList<>();
        this.args = new ArrayList<>();
        this.listTypeRef = new TypeReference<List<String>>() {};
    }

    public Eval cachedScript(String script) {
        this.script = script;
        return this;
    }

    public Eval appendScript(String script) {
        this.script = this.script == null ? script : this.script + script;
        return this;
    }
    
    public Eval addKey(String key) {
        keys.add(key);
        return this;
    }
    
    public Eval addKeys(String... keys) {
        for (String key : keys) {
            this.keys.add(key);
        }
        return this;
    }
    
    public Eval addKeys(List<String> keys) {
        for (String key : keys) {
            this.keys.add(key);
        }
        return this;
    }
    
    public Eval deserializeAndAddKeys(String serializedKeys) {
        try {
			keys.addAll(formatter.deserialize(serializedKeys, listTypeRef));
		} catch (RcDeserializationException e) {
			throw new RedisCommandoException("Unable to deserialize " + serializedKeys + " as List<String>");
		}
        return this;
    }
    
    
    public Eval addArg(String arg) {
        this.args.add(arg);
        return this;
    }
    
    public Eval addArg(double arg) {
        this.args.add(String.valueOf(arg));
        return this;
    }
    
    public Eval addArg(int arg) {
        this.args.add(String.valueOf(arg));
        return this;
    }
    
    public Eval addArg(long arg) {
        this.args.add(String.valueOf(arg));
        return this;
    }
    
    public Eval addArgs(String... args) {
        for (String arg : args) {
            this.args.add(arg);
        }
        return this;
    }
    
    public Eval addArgs(Collection<String> args) {
        this.args.addAll(args);
        return this;
    }
    
    public Eval addArg(Object toSerialize) {
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
    	return rcommando.executeScript(script, outputType, keys(), args());
    }
    
    protected String[] keys() {
        return this.keys.toArray(arrayRef);
    }
    
    protected String[] args() {
        return this.args.toArray(arrayRef);
    }
}
