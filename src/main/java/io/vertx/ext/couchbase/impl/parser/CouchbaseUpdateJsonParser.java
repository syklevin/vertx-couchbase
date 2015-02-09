package io.vertx.ext.couchbase.impl.parser;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Map;
import java.util.function.Function;

/**
 * Created by tommykwan on 6/2/15.
 */
public class CouchbaseUpdateJsonParser {

    public static JsonObject updateJsonObject(JsonObject old, JsonArray actions) {
        JsonObject result = null;
        for (Object action : actions) {
            result = updateJsonObject(old, (JsonObject)action);
        }
        return result;
    }

    public static JsonObject updateJsonObject(JsonObject old, JsonObject action) {
        Map.Entry<String, Object> next = action.getMap().entrySet().iterator().next();
        String key = next.getKey();
        JsonObject updateObj = (JsonObject)next.getValue();

        Map.Entry<String, Object> updateObjNext = updateObj.getMap().entrySet().iterator().next();
        String updateKey = updateObjNext.getKey();
        Object updateObject = updateObjNext.getValue();
        Function<Object, Object> fn = (o) -> o;
        switch (key) {
            case "$set":
                fn = (o) -> updateObject;
                break;
            case "$push":
                fn = (o) -> {
                    if (o == null) {
                        o = new JsonArray();
                    }
                    JsonArray arr = (JsonArray) o;
                    if (updateObject instanceof JsonArray) {
                        for (Object uo : (JsonArray) updateObject) {
                            arr.add(uo);
                        }
                    } else {
                        arr.add(updateObject);
                    }
                    return arr;
                };
                break;
            case "$pull":
                fn = (o) -> {
                    if (o == null) {
                        o = new JsonArray();
                    }
                    JsonArray arr = (JsonArray) o;
                    if (updateObject instanceof JsonArray) {
                        for (Object uo : (JsonArray) updateObject) {
                            arr.remove(uo);
                        }
                    } else {
                        arr.remove(updateObject);
                    }
                    return arr;
                };
                break;
            case "$addToSet":
                fn = (o) -> {
                    if (o == null) {
                        o = new JsonArray();
                    }
                    JsonArray arr = (JsonArray) o;
                    if (updateObject instanceof JsonArray) {
                        for (Object uo : (JsonArray)updateObject) {
                            if (!arr.contains(uo)) {
                                return arr.add(uo);
                            }
                        }
                    } else {
                        arr.add(updateObject);
                    }
                    return arr;
                };
                break;
        }
        setPathValue(old, updateKey, fn);
        return old;
    }

    public static void setPathValue(JsonObject o, String path, Function<Object, Object> fn) {
        if (!path.contains(".")) {
            o.put(path, fn.apply(o.getValue(path, null)));
        } else {
            String prefix = path.split("\\.")[0];
            String newPath = path.substring(prefix.length() + 1, path.length());
            if (!o.containsKey(prefix)) {
                o.put(prefix, new JsonObject());
            }
            o = o.getJsonObject(prefix);
            setPathValue(o, newPath, fn);
        }
    }

}
