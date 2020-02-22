package com.jarcadia.rcommando.callbacks;

@FunctionalInterface
public interface DaoDeleteCallback {

    public void onDelete(String mapKey, String id);

}