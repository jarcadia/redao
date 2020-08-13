package com.jarcadia.rcommando.callbacks;

import com.jarcadia.rcommando.Dao;

@FunctionalInterface
public interface DaoInsertedCallback {

    public void onInsert(Dao dao);

}