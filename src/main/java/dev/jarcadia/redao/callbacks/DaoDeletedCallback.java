package dev.jarcadia.redao.callbacks;

@FunctionalInterface
public interface DaoDeletedCallback {

    public void onDelete(String mapKey, String id);

}