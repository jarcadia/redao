//package com.jarcadia.rcommando;
//
//public class SingleFieldChangeResult {
//
//    private final Dao dao;
//    private final long version;
//    private final String field;
//    private final DaoValue before;
//    private final DaoValue after;
//    private final boolean isInsert;
//
//    protected SingleFieldChangeResult(Dao dao, long version, String field, DaoValue before, DaoValue after) {
//    	this.dao = dao;
//        this.version = version;
//        this.field = field;
//        this.before = before;
//        this.after = after;
//        this.isInsert = version == 1L;
//    }
//
//    public Dao getDao() {
//        return dao;
//    }
//
//    public long getVersion() {
//        return version;
//    }
//
//    public String getField() {
//        return field;
//    }
//
//    public DaoValue getBefore() {
//        return before;
//    }
//
//    public DaoValue getAfter() {
//        return after;
//    }
//
//    public boolean isInsert() {
//        return isInsert;
//    }
//}
