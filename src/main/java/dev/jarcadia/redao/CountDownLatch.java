package dev.jarcadia.redao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.lettuce.core.KeyValue;

public class CountDownLatch {
    
    private final RedaoCommando rcommando;
    private final ValueFormatter formatter;
    private final String hashKey;
    
    protected CountDownLatch(RedaoCommando rcommando, ValueFormatter formatter, String id) {
        this.rcommando = rcommando;
        this.formatter = formatter;
        this.hashKey =  "cdl." + id;
    }
    
    public void init(int count, Object... fieldsAndValues) {
        Map<String, String> args = prepareArgs(fieldsAndValues);
        args.put("remaining",  String.valueOf(count));
        rcommando.core().hmset(this.hashKey, args);
    }

    public boolean decrement() {
        long remaining = rcommando.core().hincrby(this.hashKey, "remaining", -1);
        if (remaining == 0) {
            rcommando.core().del(this.hashKey);
            return true;
        } else {
            return false;
        }
    }

    public Optional<DaoValue> decrement(String field) {
        long remaining = rcommando.core().hincrby(this.hashKey, "remaining", -1);
        if (remaining == 0) {
            DaoValue value = new DaoValue(formatter, field, rcommando.core().hget(hashKey, field));
            rcommando.core().del(this.hashKey);
            return Optional.of(value);
        }
        return Optional.empty();
    }

    public Optional<DaoValues> decrement(String... fields) {
        long remaining = rcommando.core().hincrby(this.hashKey, "remaining", -1);
        if (remaining == 0) {
            List<KeyValue<String, String>> values = rcommando.core().hmget(this.hashKey, fields);
            rcommando.core().del(this.hashKey);
            return Optional.of(new DaoValues(formatter, values));
        }
        return Optional.empty();
    }
    
    private Map<String, String> prepareArgs(Object[] fieldsAndValues) {
        if (fieldsAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("A value must be specified for each field name");
        }
        Map<String, String> args = new HashMap<>();
        for (int i=0; i<fieldsAndValues.length; i+=2) {
            if (!(fieldsAndValues[i] instanceof String)) {
                throw new IllegalArgumentException("Field name must be a String");
            }
            args.put((String) fieldsAndValues[i], formatter.serialize(fieldsAndValues[i+1]));
        }
        return args;
    }
}
