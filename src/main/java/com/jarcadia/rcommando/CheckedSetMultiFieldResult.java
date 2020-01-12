package com.jarcadia.rcommando;

import java.util.List;

public class CheckedSetMultiFieldResult {

    private final String mapKey;
    private final String id;
    private final long version;
    private final List<RedisChangedValue> changes;
    private final boolean isInsert;

    public CheckedSetMultiFieldResult(String mapKey, String id, long version, List<RedisChangedValue> changes) {
        this.mapKey = mapKey;
        this.id = id;
        this.version = version;
        this.changes = changes;
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

    public List<RedisChangedValue> getChanges() {
        return changes;
    }
    
    public boolean isInsert() {
        return isInsert;
    }
}
