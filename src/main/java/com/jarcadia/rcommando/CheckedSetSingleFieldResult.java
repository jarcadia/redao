package com.jarcadia.rcommando;

public class CheckedSetSingleFieldResult {

    private final String setKey;
    private final String id;
    private final long version;
    private final String field;
    private final RcValue before;
    private final RcValue after;
    private final boolean isInsert;

    public CheckedSetSingleFieldResult(String setKey, String id, long version, String field, RcValue before, RcValue after) {
        this.setKey = setKey;
        this.id = id;
        this.version = version;
        this.field = field;
        this.before = before;
        this.after = after;
        this.isInsert = version == 1L;
    }

    public String getSetKey() {
        return setKey;
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

    public RcValue getBefore() {
        return before;
    }

    public RcValue getAfter() {
        return after;
    }

    public boolean isInsert() {
        return isInsert;
    }
}
