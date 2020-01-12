package com.jarcadia.rcommando;

public class CheckedSetSingleFieldResult {

    private final String mapKey;
    private final String id;
    private final long version;
    private final String field;
    private final RedisValue before;
    private final RedisValue after;
    private final boolean isInsert;

    public CheckedSetSingleFieldResult(String mapKey, String id, long version, String field, RedisValue before, RedisValue after) {
        this.mapKey = mapKey;
        this.id = id;
        this.version = version;
        this.field = field;
        this.before = before;
        this.after = after;
        this.isInsert = version == 1L;
    }

    public String getMapKey() {
        return mapKey;
    }

    public String getId() {
        return id;
    }

    public long getVersion() {
        return version;
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

    public boolean isInsert() {
        return isInsert;
    }
}
