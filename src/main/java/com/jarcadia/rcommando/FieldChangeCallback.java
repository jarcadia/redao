package com.jarcadia.rcommando;

@FunctionalInterface
public interface FieldChangeCallback {

    public void onChange(String mapKey, String id, long version, String field, RcValue before, RcValue after);

}