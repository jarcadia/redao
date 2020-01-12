package com.jarcadia.rcommando;

@FunctionalInterface
public interface CheckedDeleteHandler {

    public void onDelete(String mapKey, String id);

}