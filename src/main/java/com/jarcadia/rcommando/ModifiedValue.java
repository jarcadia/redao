package com.jarcadia.rcommando;

public class ModifiedValue {

    private final String field;
    private final DaoValue before;
    private final DaoValue after;

    protected ModifiedValue(String field, DaoValue before, DaoValue after) {
        this.field = field;
        this.before = before;
        this.after = after;
    }

    public String getField() {
        return field;
    }

    public DaoValue getBefore() {
        return before;
    }

    public DaoValue getAfter() {
        return after;
    }
}
