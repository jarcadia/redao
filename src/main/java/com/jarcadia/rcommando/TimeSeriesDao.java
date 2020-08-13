package com.jarcadia.rcommando;

public class TimeSeriesDao extends Dao {

    protected TimeSeriesDao(RedisCommando rcommando, ValueFormatter formatter, String setKey, String id) {
        super(rcommando, formatter, setKey, id);
    }

}
