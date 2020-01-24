package com.jarcadia.rcommando;

public class RedisChangedValue {

    private final String field;
    private final RcValue before;
    private final RcValue after;

    protected RedisChangedValue(String field, RcValue before, RcValue after) {
        this.field = field;
        this.before = before;
        this.after = after;
    }

    public String getField() {
        return field;
    }

    public RcValue getBefore() {
        return before;
    }

    public RcValue getAfter() {
        return after;
    }
}
