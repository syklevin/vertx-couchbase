package io.vertx.ext.couchbase.impl.parser;

import com.couchbase.client.java.view.ViewQuery;

/**
 * Created by tommykwan on 16/2/15.
 */
public class ViewQueryKeyParser {

    public static class ViewQueryKeyParserException extends Exception {
        public ViewQueryKeyParserException(String key) {
            super(key + " is invalid type");
        }
    }

    public static void parseKey(ViewQuery viewQuery, Object object) throws Exception {
        if (object instanceof String) {
            viewQuery.key((String) object);
        } else if (object instanceof Long) {
            viewQuery.key((Long)object);
        } else if (object instanceof Integer) {
            viewQuery.key((Integer)object);
        } else if (object instanceof Double) {
            viewQuery.key((Double)object);
        } else if (object instanceof Boolean) {
            viewQuery.key((Boolean)object);
        } else if (object instanceof io.vertx.core.json.JsonObject) {
            viewQuery.key(CouchbaseVertxJsonConverter.convertCouchbaseJsonObject((io.vertx.core.json.JsonObject) object));
        } else if (object instanceof io.vertx.core.json.JsonArray) {
            viewQuery.key(CouchbaseVertxJsonConverter.convertCouchbaseJsonArray((io.vertx.core.json.JsonArray) object));
        } else {
            throw new ViewQueryKeyParserException("key");
        }
    }

    public static void parseEndKey(ViewQuery viewQuery, Object object) throws Exception {
        if (object instanceof String) {
            viewQuery.endKey((String) object);
        } else if (object instanceof Long) {
            viewQuery.endKey((Long) object);
        } else if (object instanceof Integer) {
            viewQuery.endKey((Integer) object);
        } else if (object instanceof Double) {
            viewQuery.endKey((Double) object);
        } else if (object instanceof Boolean) {
            viewQuery.endKey((Boolean) object);
        } else if (object instanceof io.vertx.core.json.JsonObject) {
            viewQuery.endKey(CouchbaseVertxJsonConverter.convertCouchbaseJsonObject((io.vertx.core.json.JsonObject) object));
        } else if (object instanceof io.vertx.core.json.JsonArray) {
            viewQuery.endKey(CouchbaseVertxJsonConverter.convertCouchbaseJsonArray((io.vertx.core.json.JsonArray) object));
        } else {
            throw new ViewQueryKeyParserException("endKey");
        }
    }

    public static void parseStartKey(ViewQuery viewQuery, Object object) throws Exception {
        if (object instanceof String) {
            viewQuery.startKey((String) object);
        } else if (object instanceof Long) {
            viewQuery.startKey((Long) object);
        } else if (object instanceof Integer) {
            viewQuery.startKey((Integer) object);
        } else if (object instanceof Double) {
            viewQuery.startKey((Double) object);
        } else if (object instanceof Boolean) {
            viewQuery.startKey((Boolean) object);
        } else if (object instanceof io.vertx.core.json.JsonObject) {
            viewQuery.startKey(CouchbaseVertxJsonConverter.convertCouchbaseJsonObject((io.vertx.core.json.JsonObject) object));
        } else if (object instanceof io.vertx.core.json.JsonArray) {
            viewQuery.startKey(CouchbaseVertxJsonConverter.convertCouchbaseJsonArray((io.vertx.core.json.JsonArray) object));
        } else {
            throw new ViewQueryKeyParserException("startKey");
        }
    }

}
