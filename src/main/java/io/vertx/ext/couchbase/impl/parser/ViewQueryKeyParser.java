package io.vertx.ext.couchbase.impl.parser;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
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

    public static void parseKey(ViewQuery viewQuery, Object object) throws ViewQueryKeyParserException {
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
            viewQuery.key(JsonObject.from(((io.vertx.core.json.JsonObject)object).getMap()));
        } else if (object instanceof io.vertx.core.json.JsonArray) {
            viewQuery.key(JsonArray.from(((io.vertx.core.json.JsonArray) object).getList()));
        } else {
            throw new ViewQueryKeyParserException("key");
        }
    }

    public static void parseEndKey(ViewQuery viewQuery, Object object) throws ViewQueryKeyParserException {
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
            viewQuery.key(JsonObject.from(((io.vertx.core.json.JsonObject)object).getMap()));
        } else if (object instanceof io.vertx.core.json.JsonArray) {
            viewQuery.key(JsonArray.from(((io.vertx.core.json.JsonArray) object).getList()));
        } else {
            throw new ViewQueryKeyParserException("endKey");
        }
    }

    public static void parseStartKey(ViewQuery viewQuery, Object object) throws ViewQueryKeyParserException {
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
            viewQuery.key(JsonObject.from(((io.vertx.core.json.JsonObject)object).getMap()));
        } else if (object instanceof io.vertx.core.json.JsonArray) {
            viewQuery.key(JsonArray.from(((io.vertx.core.json.JsonArray) object).getList()));
        } else {
            throw new ViewQueryKeyParserException("startKey");
        }
    }
}
