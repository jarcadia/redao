package dev.jarcadia.redao;

import java.util.List;

public class Modification {
	
	public static final Modification UNCHANGED = null;
	
	private final Dao dao;
    private final List<ModifiedValue> changes;
    private final boolean inserted;

    protected Modification(Dao dao, boolean inserted, List<ModifiedValue> changes) {
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

    public List<ModifiedValue> getChanges() {
        return changes;
    }
}
