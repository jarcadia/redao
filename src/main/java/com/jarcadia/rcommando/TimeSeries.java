package com.jarcadia.rcommando;

import java.util.UUID;

public class TimeSeries extends Index {

    protected TimeSeries(RedisCommando rcommando, ValueFormatter formatter, String seriesKey) {
        super(rcommando, formatter, seriesKey);
    }

    public void insert(Object... fieldsAndValues) {
        Dao dao = this.get(UUID.randomUUID().toString());
        dao.set(System.currentTimeMillis(), fieldsAndValues);
    }

    // TODO implement GET
}
