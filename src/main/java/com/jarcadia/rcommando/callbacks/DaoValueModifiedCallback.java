package com.jarcadia.rcommando.callbacks;

import com.jarcadia.rcommando.Dao;
import com.jarcadia.rcommando.DaoValue;

@FunctionalInterface
public interface DaoValueModifiedCallback {

    public void onChange(Dao dao, String field, DaoValue before, DaoValue after);

}