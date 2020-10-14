package dev.jarcadia.redao.callbacks;

import dev.jarcadia.redao.Dao;

@FunctionalInterface
public interface DaoInsertedCallback {

    public void onInsert(Dao dao);

}