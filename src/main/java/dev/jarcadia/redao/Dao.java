package dev.jarcadia.redao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import dev.jarcadia.redao.proxy.Proxy;

import io.lettuce.core.KeyValue;

public class Dao {
    
    private final RedaoCommando rcommando;
    private final ValueFormatter formatter;
    private final String type;
    private final String path;
    private final String id;

    protected Dao(RedaoCommando rcommando, ValueFormatter formatter, String type, String id) {
        this.rcommando = rcommando;
        this.formatter = formatter;
        this.type = type;
        this.path = type + "/" + id;
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public String getPath() { return path; }
    
    public boolean exists() {
    	return rcommando.core().exists(this.path) == 1L;
    }

    public <T extends Proxy> T as(Class<T> proxyClass) {
    	return rcommando.createObjectProxy(this, proxyClass);
    }

    public DaoValue get(String field) {
        return new DaoValue(formatter, field, rcommando.core().hget(path, field));
    }

    public DaoValues get(String... fields) {
        List<KeyValue<String, String>> values = rcommando.core().hmget(this.path, fields);
        return new DaoValues(formatter, values);
    }

    public DaoValues getAll() {
        Map<String, String> values = rcommando.core().hgetall(this.path);
        return new DaoValues(formatter, values);
    }

    public Optional<Modification> set(Object... fieldsAndValues) {
        return this.setHelper(0, fieldsAndValues);
    }

    public Optional<Modification> setTs(Object... fieldsAndValues) {
        long now = System.currentTimeMillis();
        Object[] fieldsAndValuesWithTimestamp = Arrays.copyOf(fieldsAndValues, fieldsAndValues.length + 2);
        fieldsAndValuesWithTimestamp[fieldsAndValues.length] = "timestamp";
        fieldsAndValuesWithTimestamp[fieldsAndValues.length + 1] = now;
        return this.setHelper(now, fieldsAndValuesWithTimestamp);
    }

    private Optional<Modification> setHelper(long score, Object... fieldsAndValues) {
    	List<String> bulkChanges = rcommando.eval()
                .cachedScript(Scripts.DAO_SET)
                .addKeys(Keys.TYPES, this.type, this.path, this.type +".change")
                .addArg(score)
                .addArgs(prepareArgsAsArray(fieldsAndValues))
                .returnMulti();

        if (bulkChanges.size() > 0) {
            List<ModifiedValue> changes = new ArrayList<>();
            long version = Long.parseLong(bulkChanges.get(0));
            for (int i=1; i<bulkChanges.size(); i+=3) {
                ModifiedValue changedValue = new ModifiedValue(bulkChanges.get(i),
                        new DaoValue(formatter, bulkChanges.get(i), bulkChanges.get(i+1)),
                        new DaoValue(formatter, bulkChanges.get(i), bulkChanges.get(i+2)));
                changes.add(changedValue);
            }
            Modification result = new Modification(this, version == 1L, changes);
            rcommando.invokeChangeCallbacks(result);
            return Optional.of(result);
        } else {
            return Optional.empty();
        }
    }

    public Optional<Modification> setAll(Map<String, Object> properties) {
        return setAll(properties.entrySet());
    }

    public Optional<Modification> setAll(Collection<Map.Entry<String, Object>> properties) {
        return setAll(properties.stream());
    }

    public Optional<Modification> setAll(Stream<Map.Entry<String, Object>> properties) {
        Object[] fieldsAndValues = properties
                .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList())
                .toArray(new Object[0]);
        return fieldsAndValues.length > 0 ? this.set(fieldsAndValues) : Optional.empty();
    }

    public boolean touch() {
        boolean created = rcommando.eval()
            .cachedScript(Scripts.DAO_TOUCH)
            .addKeys(Keys.TYPES, this.type, this.path, this.type + ".change")
            .addArg(0)
            .returnLong() == 1L;
        if (created) {
            rcommando.invokeObjectInsertCallbacks(this);
        }
        return created;
    }

    public boolean delete() {
        int numDeleted = rcommando.eval()
                .cachedScript(Scripts.DAO_CHECKED_DELETE)
                .addKeys(Keys.TYPES, this.type, this.path, this.type + ".change")
                .returnInt();
        
        if (numDeleted == 1) {
            rcommando.invokeDeleteCallbacks(type, id);
            return true;
        } else {
            return false;
        }
    }

    public Optional<Modification> clear(String... fields) {
        List<String> bulkChanges = rcommando.eval()
                .cachedScript(Scripts.DAO_CLEAR_FIELD)
                .addKeys(this.type, this.path, this.type +".change")
                .addArgs(fields)
                .returnMulti();

        if (bulkChanges.size() > 0) {
            List<ModifiedValue> changes = new ArrayList<>();
            long version = Long.parseLong(bulkChanges.get(0));
            for (int i=1; i<bulkChanges.size(); i+=2) {
                ModifiedValue changedValue = new ModifiedValue(bulkChanges.get(i),
                        new DaoValue(formatter, bulkChanges.get(i), bulkChanges.get(i+1)),
                        new DaoValue(formatter, bulkChanges.get(i), null));
                changes.add(changedValue);
            }
            Modification result = new Modification(this, false, changes);
            rcommando.invokeChangeCallbacks(result);
            return Optional.of(result);
        } else {
        	return Optional.empty();
        }
    }
    
    private String[] prepareArgsAsArray(Object[] fieldsAndValues) {
        if (fieldsAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("A value must be specified for each field name");
        }
        String[] args = new String[fieldsAndValues.length];
        int argsIdx = 0;
        int nullCount = 0;
        for (int i=0; i<fieldsAndValues.length; i+=2) {
        	if (fieldsAndValues[i+1] == null) {
        		nullCount++;
        	} else {
                if (fieldsAndValues[i] instanceof String) {
                    args[argsIdx++] = (String) fieldsAndValues[i];
                    args[argsIdx++] = formatter.serialize(fieldsAndValues[i+1]);
                } else {
                    throw new IllegalArgumentException("Field name is set operation must be a String");
                }
        	}
        }
        if (nullCount > 0) {
        	return Arrays.copyOf(args, args.length - nullCount * 2);
        } else {
            return args;
        }
    }
    
    @Override
    public int hashCode() {
    	return Objects.hash(type, id);
    }

    @Override
    public boolean equals(Object obj) {
    	return this.hashCode() == obj.hashCode();
    }
    
    @Override
    public String toString() {
    	return this.path;
    }
}
