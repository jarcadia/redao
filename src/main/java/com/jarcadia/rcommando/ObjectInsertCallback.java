package com.jarcadia.rcommando;

@FunctionalInterface
public interface ObjectInsertCallback {

    public void onInsert(String mapKey, String id);

}