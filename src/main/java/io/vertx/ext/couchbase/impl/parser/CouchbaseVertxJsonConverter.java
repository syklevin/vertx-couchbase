package io.vertx.ext.couchbase.impl.parser;


import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.transcoder.JsonTranscoder;

/**
 * Created by tommykwan on 25/2/15.
 */
public class CouchbaseVertxJsonConverter {

    public static JsonObject convertCouchbaseJsonObject(io.vertx.core.json.JsonObject input) throws Exception {
        return new JsonTranscoder().stringToJsonObject(input.encode());
    }

    public static JsonArray convertCouchbaseJsonArray(io.vertx.core.json.JsonArray input) throws Exception {
        io.vertx.core.json.JsonObject obj = new io.vertx.core.json.JsonObject().put("a", input);
        JsonObject jsonObject = convertCouchbaseJsonObject(obj);
        return jsonObject.getArray("a");
    }
}
