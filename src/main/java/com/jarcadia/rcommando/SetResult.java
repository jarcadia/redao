package com.jarcadia.rcommando;

import java.util.List;

public class SetResult {
	
	public static final SetResult UNCHANGED = null;
	
	private final Dao dao;
    private final List<Change> changes;
    private final boolean inserted;

    protected SetResult(Dao dao, boolean inserted, List<Change> changes) {
    	this.dao = dao;
        this.changes = changes;
        this.inserted = inserted;
    }

    public Dao getDao() {
        return dao;
    }
    
    public boolean isInsert() {
        return inserted;
    }

    public List<Change> getChanges() {
        return changes;
    }
}
