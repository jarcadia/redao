package com.jarcadia.rcommando;

@FunctionalInterface
public interface ObjectDeleteCallback {

    public void onDelete(String mapKey, String id);

}