package com.jarcadia.rcommando.callbacks;

@FunctionalInterface
public interface DaoDeletedCallback {

    public void onDelete(String mapKey, String id);

}