package com.jarcadia.rcommando;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

import io.lettuce.core.KeyValue;

public class RcObject {
    
    private final RedisCommando rcommando;
    private final RedisValueFormatter formatter;
    private final String setKey;
    private final String hashKey;
    private final String id;

    public RcObject(RedisCommando rcommando, RedisValueFormatter formatter, String setKey, String id) {
        this.rcommando = rcommando;
        this.formatter = formatter;
        this.setKey = setKey;
        this.hashKey = setKey + "." + id;
        this.id = id;
    }

    public String getSetKey() {
        return setKey;
    }

    public String getId() {
        return id;
    }
    
    public boolean exists() {
    	return rcommando.core().exists(this.hashKey) == 1L;
    }

    public long getVersion() {
        return this.get("v").asLong();
    }

    public RcValue get(String field) {
        return new RcValue(formatter, rcommando.core().hget(hashKey, field));
    }

    public RcValues get(String... fields) {
        List<KeyValue<String, String>> values = rcommando.core().hmget(this.hashKey, fields);
        return  new RcValues(formatter, values.iterator());
    }

    public void set(Object... fieldsAndValues) {
    	rcommando.core().hmset(this.hashKey, prepareArgsAsMap(fieldsAndValues));
    }
    
    public Optional<CheckedSetSingleFieldResult> checkedSet(String field, Object value) {
        List<String> bulkChanges = rcommando.eval()
                .cachedScript(Scripts.OBJ_CHECKED_SET)
                .addKeys(this.setKey, this.hashKey, this.setKey+".change")
                .addArgs(prepareArgsAsArray(new Object[] {field, value}))
                .returnMulti();

        if (bulkChanges.size() == 4) {
            RcValue before = new RcValue(formatter, bulkChanges.get(2));
            RcValue after = new RcValue(formatter, bulkChanges.get(3));
            CheckedSetSingleFieldResult result = new CheckedSetSingleFieldResult(setKey, id, Long.parseLong(bulkChanges.get(0)), bulkChanges.get(1), before, after);
            rcommando.invokeChangeCallbacks(result);
            return Optional.of(result); 
        } else {
            return Optional.empty();
        }
    }

    public Optional<CheckedSetMultiFieldResult> checkedSet(String field, Object value, Object... fieldsAndValues) {
        List<String> bulkChanges = rcommando.eval()
                .cachedScript(Scripts.OBJ_CHECKED_SET)
                .addKeys(this.setKey, this.hashKey, this.setKey+".change")
                .addArgs(field)
                .addArg(value)
                .addArgs(prepareArgsAsArray(fieldsAndValues))
                .returnMulti();

        if (bulkChanges.size() > 0) {
            List<RedisChangedValue> changes = new ArrayList<>();
            long version = Long.parseLong(bulkChanges.get(0));
            for (int i=1; i<bulkChanges.size(); i+=3) {
                RedisChangedValue changedValue = new RedisChangedValue(bulkChanges.get(i),
                        new RcValue(formatter, bulkChanges.get(i+1)),
                        new RcValue(formatter, bulkChanges.get(i+2)));
                changes.add(changedValue);
            }
            CheckedSetMultiFieldResult result = new CheckedSetMultiFieldResult(setKey, id, version, changes);
            rcommando.invokeChangeCallbacks(result);
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

    public boolean checkedTouch() {
        boolean created = rcommando.eval()
            .cachedScript(Scripts.OBJ_CHECKED_TOUCH)
            .addKeys(this.setKey, this.hashKey, this.setKey + ".change")
            .returnLong() == 1L;
        if (created) {
            rcommando.invokeObjectInsertCallbacks(setKey, id);
        }
        return created;
    }

    public boolean checkedDelete() {
        int numDeleted = rcommando.eval()
                .cachedScript(Scripts.OBJ_CHECKED_DELETE)
                .addKeys(this.setKey, this.hashKey, this.setKey + ".change")
                .addArgs(this.id)
                .returnInt();
        
        if (numDeleted == 1) {
            rcommando.invokeDeleteCallbacks(setKey, id);
            return true;
        } else {
            return false;
        }
    }

    public Optional<CheckedSetSingleFieldResult> checkedClear(String field) {
        List<String> bulkChanges = rcommando.eval()
                .cachedScript(Scripts.OBJ_CHECKED_DEL_FIELD)
                .addKeys(this.setKey, this.hashKey, this.setKey+".change")
                .addArgs(new String[] {field})
                .returnMulti();

        if (bulkChanges.size() == 3) {
            RcValue before = new RcValue(formatter, bulkChanges.get(2));
            RcValue after = new RcValue(formatter, null);
            CheckedSetSingleFieldResult result = new CheckedSetSingleFieldResult(setKey, id, Long.parseLong(bulkChanges.get(0)), bulkChanges.get(1), before, after);
            rcommando.invokeChangeCallbacks(result);
            return Optional.of(result); 
        } else {
            return Optional.empty();
        }
    }

    public Optional<CheckedSetMultiFieldResult> checkedClear(String field, String... fields) {
        List<String> bulkChanges = rcommando.eval()
                .cachedScript(Scripts.OBJ_CHECKED_DEL_FIELD)
                .addKeys(this.setKey, this.hashKey, this.setKey+".change")
                .addArg(field)
                .addArgs(fields)
                .returnMulti();

        if (bulkChanges.size() > 0) {
            System.out.println(bulkChanges);
            List<RedisChangedValue> changes = new ArrayList<>();
            long version = Long.parseLong(bulkChanges.get(0));
            for (int i=1; i<bulkChanges.size(); i+=2) {
                RedisChangedValue changedValue = new RedisChangedValue(bulkChanges.get(i),
                        new RcValue(formatter, bulkChanges.get(i+1)),
                        new RcValue(formatter, null));
                changes.add(changedValue);
            }
            CheckedSetMultiFieldResult result = new CheckedSetMultiFieldResult(setKey, id, version, changes);
            rcommando.invokeChangeCallbacks(result);
            return Optional.of(result);
        } else {
            return Optional.empty();
        }
    }
    
    private String[] prepareArgsAsArray(Object[] fieldsAndValues) {
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
                args[i] = formatter.serialize(fieldsAndValues[i]);
            }
        }
        return args;
    }
    
    private Map<String, String> prepareArgsAsMap(Object[] fieldsAndValues) {
        if (fieldsAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("A value must be specified for each field name");
        }
        Map<String, String> args = new LinkedHashMap<>();
        for (int i=0; i<fieldsAndValues.length; i+=2) {
            if (i % 2 == 0) {
                // Process field
                if (fieldsAndValues[i] instanceof String) {
                    String key = (String) fieldsAndValues[i];
                    String value = formatter.serialize(fieldsAndValues[i+1]);
                    args.put(key, value);
                } else {
                    throw new IllegalArgumentException("Field name is set operation must be a String");
                }
            }
        }
        return args;
    }

    @Override
    public int hashCode() {
    	return Objects.hash(setKey, id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RcObject other = (RcObject) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (setKey == null) {
            if (other.setKey != null)
                return false;
        } else if (!setKey.equals(other.setKey))
            return false;
        return true;
    }
}
