package io.vertx.ext.couchbase;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.couchbase.impl.parser.CouchbaseVertxJsonConverter;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by tommykwan on 25/2/15.
 */
public class CouchbaseVertxJsonConverterTest {

    @Test
    public void testConvertJsonObject() throws Exception {
        JsonObject jsonObject = new JsonObject()
            .put("a", 1)
            .put("b", new JsonObject())
            .put("c", new JsonArray());
        com.couchbase.client.java.document.json.JsonObject jsonObject1 = CouchbaseVertxJsonConverter.convertCouchbaseJsonObject(jsonObject);
        Assert.assertEquals(1, jsonObject1.getInt("a").intValue());
        Assert.assertEquals(com.couchbase.client.java.document.json.JsonObject.create(), jsonObject1.getObject("b")); Assert.assertEquals(com.couchbase.client.java.document.json.JsonArray.create(), jsonObject1.getArray("c"));
    }

    @Test
    public void testConvertJsonArray() throws Exception {
        JsonArray jsonArray = new JsonArray()
            .add(1)
            .add(new JsonObject())
            .add(new JsonArray());
        com.couchbase.client.java.document.json.JsonArray jsonArray1 = CouchbaseVertxJsonConverter.convertCouchbaseJsonArray(jsonArray);
        Assert.assertEquals(1, jsonArray1.getInt(0).intValue());
        Assert.assertEquals(com.couchbase.client.java.document.json.JsonObject.create(), jsonArray1.getObject(1));
        Assert.assertEquals(com.couchbase.client.java.document.json.JsonArray.create(), jsonArray1.getArray(2));

    }

}
