package dev.jarcadia.redao.callbacks;

import dev.jarcadia.redao.Dao;
import dev.jarcadia.redao.DaoValue;

@FunctionalInterface
public interface DaoValueModifiedCallback {

    public void onChange(Dao dao, String field, DaoValue before, DaoValue after);

}