package com.jarcadia.rcommando;

public class RedisChangedValue {

    private final String field;
    private final RedisValue before;
    private final RedisValue after;

    protected RedisChangedValue(String field, RedisValue before, RedisValue after) {
        this.field = field;
        this.before = before;
        this.after = after;
    }

    public String getField() {
        return field;
    }

    public RedisValue getBefore() {
        return before;
    }

    public RedisValue getAfter() {
        return after;
    }
}
