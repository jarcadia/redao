package com.jarcadia.rcommando;

@FunctionalInterface
public interface CheckedInsertHandler {

    public void onInsert(String mapKey, String id);

}