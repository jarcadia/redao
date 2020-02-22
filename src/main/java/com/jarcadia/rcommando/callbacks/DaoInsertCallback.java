package com.jarcadia.rcommando.callbacks;

import com.jarcadia.rcommando.Dao;

@FunctionalInterface
public interface DaoInsertCallback {

    public void onInsert(Dao dao);

}