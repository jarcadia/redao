package com.jarcadia.rcommando;

public class Change {

    private final String field;
    private final DaoValue before;
    private final DaoValue after;

    protected Change(String field, DaoValue before, DaoValue after) {
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
