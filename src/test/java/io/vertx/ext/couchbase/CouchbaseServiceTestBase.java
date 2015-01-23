package io.vertx.ext.couchbase;


import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Created by levin on 10/27/2014.
 */
public class CouchbaseServiceTestBase extends VertxTestBase {

    protected CouchbaseService cbService;

    protected JsonObject getConfig() {
        JsonObject couchbaseCfg = new JsonObject()
                .put("bucket", "default")
                .put("password", "")
                .put("nodes", new JsonArray().add("127.0.0.1"));
        return couchbaseCfg;
    }


    @Test
    public void testDbInfo() throws Exception {
        cbService
                .dbInfo(new JsonObject(), ar -> {
                    if (ar.succeeded()) {
                        String status = ar.result().getString("status");
                        assertEquals(status, "ok");
                        testComplete();
                    } else {
                        fail(ar.cause().getMessage());
                    }
                });
        await();
    }

    @Test
    public void testFindOne() throws Exception {

        JsonObject command = new JsonObject();
        command.put("doctype", "user");
        command.put("id", "0ce06b76-856f-48a9-8669-d9b8a3f5ff2c");

        cbService
                .findOne(command, ar -> {
                    if (ar.succeeded()) {
                        JsonObject result = ar.result();
                        if ("ok".equals(result.getString("status"))) {
                            JsonObject dbUser = result.getJsonObject("result");
                            assertEquals(dbUser.getString("username"), "xyz");
                            testComplete();
                        } else {
                            fail(result.getString("status"));
                        }
                    } else {
                        fail(ar.cause().getMessage());
                    }
                });
        await();
    }


    @Test
    public void testInsert() {
        JsonObject command = new JsonObject();
        command.put("doctype", "user");
        command.put("id", UUID.randomUUID().toString());
        JsonObject data = new JsonObject();
        data.put("username", "abc");
        data.put("password", "123456");
        data.put("email", "abc@abc.com");

        command.put("content", data);

        cbService
                .insert(command, ar -> {
                    if (ar.succeeded()) {
                        JsonObject result = ar.result();
                        if ("ok".equals(result.getString("status"))) {
                            JsonObject dbUser = result.getJsonObject("result");
                            assertEquals(dbUser.getString("username"), "abc");
                            assertEquals(dbUser.getString("password"), "123456");
                            testComplete();
                        } else {
                            fail(result.getString("status"));
                        }
                    } else {
                        fail(ar.cause().getMessage());
                    }
                });

        await();
    }

    @Test
    public void testN1ql(){

        JsonObject command = new JsonObject();
        command.put("query", "SELECT * FROM default WHERE username = 'xyz'");

        cbService.n1ql(command, ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                if ("ok".equals(result.getString("status"))) {
                    assertTrue(result.getInteger("total") >= 0);
                    testComplete();
                } else {
                    fail(result.getString("status"));
                }
            } else {
                fail(ar.cause().getMessage());
            }
        });

        await(30, TimeUnit.SECONDS);

    }

    @Test
    public void testUpdate() {

        JsonObject command = new JsonObject();
        command.put("doctype", "user");
        command.put("id", "0ce06b76-856f-48a9-8669-d9b8a3f5ff2c");
        command.put("upsert", true);
        JsonObject content = new JsonObject();
        content.put("username", "xyz");
        command.put("content", content);

        cbService.insert(command, ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                if ("ok".equals(result.getString("status"))) {
                    JsonObject dbUser = result.getJsonObject("result");
                    assertEquals(dbUser.getString("username"), "xyz");
                    testComplete();
                } else {
                    fail(result.getString("status"));
                }
            } else {
                fail(ar.cause().getMessage());
            }
        });

        await(30, TimeUnit.SECONDS);
    }
}
