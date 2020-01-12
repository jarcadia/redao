package com.jarcadia.rcommando;

@FunctionalInterface
public interface CheckedSetUpdateHandler {

    public void onChange(String mapKey, String id, long version, String field, RedisValue before, RedisValue after);

}