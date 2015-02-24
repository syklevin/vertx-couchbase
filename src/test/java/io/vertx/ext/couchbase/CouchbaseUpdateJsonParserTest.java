package io.vertx.ext.couchbase;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.couchbase.impl.parser.UpdateJsonParser;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

/**
 * Created by tommykwan on 6/2/15.
 */
public class CouchbaseUpdateJsonParserTest extends VertxTestBase {

    @Test
    public void testSet() {
        JsonObject action = new JsonObject()
            .put("$set", new JsonObject()
                .put("a", "b"));
        JsonObject a = UpdateJsonParser.updateJsonObject(new JsonObject(), action);
        assertEquals("b", a.getString("a"));
    }

    @Test
    public void testPushArray() {
        JsonObject action = new JsonObject()
            .put("$push", new JsonObject()
                .put("a", "b"));
        JsonObject a = UpdateJsonParser.updateJsonObject(new JsonObject(), action);
        assertEquals("b", a.getJsonArray("a").getValue(0));
    }

    @Test
    public void testPushArrayWithMultiItems() {
        JsonObject action2 = new JsonObject()
            .put("$push", new JsonObject()
                .put("a", new JsonArray().add("aa").add("bb")));
        JsonObject a2 = UpdateJsonParser.updateJsonObject(new JsonObject(), action2);
        assertEquals(2, a2.getJsonArray("a").size());
    }

    @Test
    public void testPullArray() {
        JsonObject o = new JsonObject()
            .put("arr", new JsonArray().add("a"));
        JsonObject action = new JsonObject()
            .put("$pull", new JsonObject()
                .put("arr", "a"));
        JsonObject a = UpdateJsonParser.updateJsonObject(o, action);
        assertEquals(0, a.getJsonArray("arr").size());
    }

    @Test
    public void testAddToSet() {
        JsonObject o = new JsonObject()
            .put("arr", new JsonArray().add("a"));
        JsonObject action = new JsonObject()
            .put("$addToSet", new JsonObject()
                .put("arr", "a"));
        JsonObject a = UpdateJsonParser.updateJsonObject(o, action);
        assertEquals(1, a.getJsonArray("arr").size());

        JsonObject action2 = new JsonObject()
            .put("$addToSet", new JsonObject()
                .put("arr", "b"));
        JsonObject a2 = UpdateJsonParser.updateJsonObject(a, action2);
        assertEquals(2, a2.getJsonArray("arr").size());
    }

    @Test
    public void testKeyPathDepth() {
        JsonObject o = new JsonObject()
            .put("a", new JsonObject());
        JsonObject action = new JsonObject()
            .put("$set", new JsonObject()
                .put("a.b.c", "a"));
        JsonObject a = UpdateJsonParser.updateJsonObject(o, action);
        assertEquals("a", a.getJsonObject("a").getJsonObject("b").getString("c"));
    }

    @Test
    public void testMultiActions() {
        JsonObject actions = new JsonObject()
            .put("$push", new JsonObject().put("a", "b").put("b", new JsonArray().add("a").add("b")));

        JsonObject b = UpdateJsonParser.updateJsonObject(new JsonObject(), actions);
        assertEquals("b", b.getJsonArray("a").getString(0));
        assertEquals("a", b.getJsonArray("b").getString(0));
        assertEquals("b", b.getJsonArray("b").getString(1));
    }

}
