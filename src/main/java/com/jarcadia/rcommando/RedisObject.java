package com.jarcadia.rcommando;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import io.lettuce.core.KeyValue;

@JsonSerialize(using = RedisObjectSerializer.class)
public class RedisObject {
    
    private final RedisCommando rcommando;
    private final RedisValueFormatter formatter;
    private final String mapKey;
    private final String hashKey;
    private final String id;

    public RedisObject(RedisCommando rcommando, RedisValueFormatter formatter, String mapKey, String id) {
        this.rcommando = rcommando;
        this.formatter = formatter;
        this.mapKey = mapKey;
        this.hashKey = mapKey + "." + id;
        this.id = id;
    }

    public String getMapKey() {
        return mapKey;
    }

    public String getId() {
        return id;
    }

    public long getVersion() {
        return this.get("v").asLong();
    }

    public RedisValue get(String field) {
        return new RedisValue(formatter, rcommando.core().hget(hashKey, field));
    }

    public RedisValues get(String... fields) {
        List<KeyValue<String, String>> values = rcommando.core().hmget(this.hashKey, fields);
        return  new RedisValues(formatter, values.iterator());
    }

    public boolean init() {
        return rcommando.eval()
            .useScriptFile("objInit")
            .addKeys(this.mapKey, this.hashKey)
            .returnLong() == 1L;
    }

    public void set(Object... fieldsAndValues) {
        rcommando.eval()
                .useScriptFile("objSet")
                .addKeys(this.mapKey, this.hashKey)
                .addArgs(prepareArgs(fieldsAndValues))
                .returnStatus();
    }
    
    public Optional<CheckedSetSingleFieldResult> checkedSet(String field, Object value) {
        List<String> bulkChanges = rcommando.eval()
                .useScriptFile("objCheckedSet")
                .addKeys(this.mapKey, this.hashKey, this.mapKey+".change")
                .addArgs(prepareArgs(new Object[] {field, value}))
                .returnMulti();

        if (bulkChanges.size() == 4) {
            RedisValue before = new RedisValue(formatter, bulkChanges.get(2));
            RedisValue after = new RedisValue(formatter, bulkChanges.get(3));
            CheckedSetSingleFieldResult result = new CheckedSetSingleFieldResult(mapKey, id, Long.parseLong(bulkChanges.get(0)), bulkChanges.get(1), before, after);
            rcommando.invokeCheckedSetCallbacks(result);
            return Optional.of(result); 
        } else {
            return Optional.empty();
        }
    }

    public Optional<CheckedSetMultiFieldResult> checkedSet(String field, Object value, Object... fieldsAndValues) {
        List<String> bulkChanges = rcommando.eval()
                .useScriptFile("objCheckedSet")
                .addKeys(this.mapKey, this.hashKey, this.mapKey+".change")
                .addArg(field)
                .addArg(value)
                .addArgs(prepareArgs(fieldsAndValues))
                .returnMulti();

        if (bulkChanges.size() > 0) {
            List<RedisChangedValue> changes = new ArrayList<>();
            long version = Long.parseLong(bulkChanges.get(0));
            for (int i=1; i<bulkChanges.size(); i+=3) {
                RedisChangedValue changedValue = new RedisChangedValue(bulkChanges.get(i),
                        new RedisValue(formatter, bulkChanges.get(i+1)),
                        new RedisValue(formatter, bulkChanges.get(i+2)));
                changes.add(changedValue);
            }
            CheckedSetMultiFieldResult result = new CheckedSetMultiFieldResult(mapKey, id, version, changes);
            rcommando.invokeCheckedSetCallbacks(result);
            return Optional.of(result);
        } else {
            return Optional.empty();
        }
    }

    public Optional<CheckedSetMultiFieldResult> checkedSet(Map<String, Object> properties) {
        if (properties.isEmpty()) {
            return Optional.empty();
        } else {
            Object[] fieldsAndValues = new Object[properties.size() * 2 - 2];
            Iterator<Entry<String, Object>> iterator = properties.entrySet().iterator();
            Entry<String, Object> entry = iterator.next();
            String firstField = entry.getKey();
            Object firstValue = entry.getValue();
            int index = 0;
            while(iterator.hasNext()) {
                entry = iterator.next();
                fieldsAndValues[index++] = entry.getKey();
                fieldsAndValues[index++] = entry.getValue();
            }
            return this.checkedSet(firstField, firstValue, fieldsAndValues);
        }
    }

    public boolean checkedInit() {
        boolean init = rcommando.eval()
            .useScriptFile("objCheckedInit")
            .addKeys(this.mapKey, this.hashKey, this.mapKey + ".change")
            .returnLong() == 1L;
        if (init) {
            rcommando.invokeCheckedInsertHandlers(mapKey, id);
        }
        return init;
    }

    public boolean checkedDelete() {
        int numDeleted = rcommando.eval()
                .useScriptFile("objCheckedDelete")
                .addKeys(this.mapKey, this.hashKey, this.mapKey + ".change")
                .addArgs(this.id)
                .returnInt();
        
        if (numDeleted == 1) {
            rcommando.invokeCheckedDeleteHandlers(mapKey, id);
            return true;
        } else {
            return false;
        }
    }

    public Optional<CheckedSetSingleFieldResult> checkedClear(String field) {
        List<String> bulkChanges = rcommando.eval()
                .useScriptFile("objClear")
                .addKeys(this.mapKey, this.hashKey, this.mapKey+".change")
                .addArgs(new String[] {field})
                .returnMulti();

        if (bulkChanges.size() == 3) {
            RedisValue before = new RedisValue(formatter, bulkChanges.get(2));
            RedisValue after = new RedisValue(formatter, null);
            CheckedSetSingleFieldResult result = new CheckedSetSingleFieldResult(mapKey, id, Long.parseLong(bulkChanges.get(0)), bulkChanges.get(1), before, after);
            rcommando.invokeCheckedSetCallbacks(result);
            return Optional.of(result); 
        } else {
            return Optional.empty();
        }
    }

    public Optional<CheckedSetMultiFieldResult> checkedClear(String field, String... fields) {
        List<String> bulkChanges = rcommando.eval()
                .useScriptFile("objClear")
                .addKeys(this.mapKey, this.hashKey, this.mapKey+".change")
                .addArg(field)
                .addArgs(fields)
                .returnMulti();

        if (bulkChanges.size() > 0) {
            System.out.println(bulkChanges);
            List<RedisChangedValue> changes = new ArrayList<>();
            long version = Long.parseLong(bulkChanges.get(0));
            for (int i=1; i<bulkChanges.size(); i+=2) {
                RedisChangedValue changedValue = new RedisChangedValue(bulkChanges.get(i),
                        new RedisValue(formatter, bulkChanges.get(i+1)),
                        new RedisValue(formatter, null));
                changes.add(changedValue);
            }
            CheckedSetMultiFieldResult result = new CheckedSetMultiFieldResult(mapKey, id, version, changes);
            rcommando.invokeCheckedSetCallbacks(result);
            return Optional.of(result);
        } else {
            return Optional.empty();
        }
    }
    
    private String[] prepareArgs(Object[] fieldsAndValues) {
        String[] args = new String[fieldsAndValues.length];
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("A value must be specified for each field name");
        }
        for (int i=0; i<args.length; i++) {
            if (i % 2 == 0) {
                // Process field
                if (fieldsAndValues[i] instanceof String) {
                    args[i] = (String) fieldsAndValues[i];
                } else {
                    throw new IllegalArgumentException("Field name is set operation must be a String");
                }
            } else {
                // Process value
                args[i] = formatter.smartSerailize(fieldsAndValues[i]);
            }
        }
        return args;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((mapKey == null) ? 0 : mapKey.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RedisObject other = (RedisObject) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (mapKey == null) {
            if (other.mapKey != null)
                return false;
        } else if (!mapKey.equals(other.mapKey))
            return false;
        return true;
    }
}
