package io.vertx.ext.couchbase;


import io.vertx.core.AsyncResult;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Created by levin on 10/27/2014.
 */
public class CouchbaseServiceTestBase extends VertxTestBase {

    protected CouchbaseService cbService;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        CountDownLatch latch = new CountDownLatch(1);
        vertx.deployVerticle("service:io.vertx:ext-couchbase", ar -> {
            if (ar.succeeded()) {
                System.out.println("success to deploy couchbase-service");
                cbService = CouchbaseService.createProxy(vertx, "vertx.couchbase");
                System.out.println("success to create service proxy");
            } else {
                System.out.println("failed to deploy couchbase-service");
                System.out.println(ar.cause().getMessage());
            }
            latch.countDown();
        });
        latch.await();
    }

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
    public void testN1ql() {

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
    public void testViewQuery() {
        JsonObject command = new JsonObject()
            .put("design", "dev_default")
            .put("view", "by_name");
        cbService.viewQuery(command, ar -> {
            JsonObject result = checkFine(ar);
            testComplete();
        });

        await();
    }

    @Test
    public void testUpdate() {
        String id = UUID.randomUUID().toString();
        String doctype = "user";
        JsonObject newOne = new JsonObject()
            .put("id", id)
            .put("doctype", doctype)
            .put("content", new JsonObject()
                .put("username", "abc")
            );
        cbService.insert(newOne, ok -> {
            JsonObject newResult = checkFine(ok);
            assertEquals("abc", newResult.getJsonObject("result").getString("username"));
            JsonObject command = new JsonObject()
                .put("upsert", true)
                .put("doctype", doctype)
                .put("id", id)
                .put("update", new JsonObject()
                    .put("$set", new JsonObject().put("username", "xyz"))
                );
            cbService.update(command, ar -> {
                JsonObject result = checkFine(ar);
                JsonObject dbUser = result.getJsonObject("result");
                assertEquals("xyz", dbUser.getString("username"));
                testComplete();
            });
        });

        await(30, TimeUnit.SECONDS);
    }


    private void checkOk(AsyncResult<JsonObject> result) {
        if (!result.succeeded())
            fail(result.cause().getMessage());
    }

    private void checkStatus(JsonObject result) {
        if (!"ok".equals(result.getString("status")))
            fail(result.getString("status"));
    }

    private JsonObject checkFine(AsyncResult<JsonObject> ar) {
        checkOk(ar);
        JsonObject result = ar.result();
        checkStatus(result);
        return result;
    }



}
