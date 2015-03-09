package io.vertx.ext.couchbase.impl;

/**
 * Created by tommykwan on 9/3/15.
 */
public enum Stale {
    TRUE("ok"),
    FALSE("false"),
    UPDATE_AFTER("update_after");

    private String identifier;

    private Stale(String identifier) {
        this.identifier = identifier;
    }

    public String identifier() {
        return this.identifier;
    }
    
    static public com.couchbase.client.java.view.Stale toStale(String id) {
        if (id.equals(TRUE.identifier)) {
            return com.couchbase.client.java.view.Stale.TRUE;
        } else if (id.equals(FALSE.identifier)){
            return com.couchbase.client.java.view.Stale.FALSE;
        } else if (id.equals(UPDATE_AFTER.identifier)) {
            return com.couchbase.client.java.view.Stale.UPDATE_AFTER;
        } else {
            return null;
        }
    }
}
