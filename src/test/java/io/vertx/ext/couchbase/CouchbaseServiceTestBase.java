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


    public JsonObject genTest() {
        String[] types = new String[] { "IT", "fashion", "boss", "manager" };
        return new JsonObject()
            .put("doctype", "test")
            .put("id", UUID.randomUUID().toString())
            .put("content", new JsonObject()
                    .put("type", types[(int)(Math.random() * types.length)])
                    .put("money", (int)(Math.random() * 1000))
            );
    }

    @Test
    public void genFixture() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(100);
        for (int i = 0; i < 100; ++i) {
            cbService.insert(genTest(), ar -> {
                latch.countDown();
                if (latch.getCount() == 0) {
                    testComplete();
                }
            });
        }
        latch.await();
    }


    @Test
    public void testViewQuery() {
        JsonObject command = new JsonObject()
            .put("design", "dev_test")
            .put("view", "test_by_id");
        cbService.viewQuery(command, ar -> {
            JsonObject result = checkFine(ar);
            assertTrue(result.getJsonArray("result").size() > 0);
            testComplete();
        });

        await();
    }

    @Test
    public void testViewQueryCount() {
        JsonObject viewquery = new JsonObject()
            .put("design", "dev_test")
            .put("view", "test_count")
            .put("reduce", true);
        cbService.viewQuery(viewquery, ar -> {
            JsonObject result = checkFine(ar);
            assertTrue(result.getJsonArray("result").getJsonObject(0).getInteger("value") > 0);
            testComplete();
        });
        await();
    }

    @Test
    public void testViewQueryGroupCount() {
        JsonObject viewquery = new JsonObject()
            .put("design", "dev_test")
            .put("view", "test_group")
            .put("group", true)
            .put("reduce", true);
        cbService.viewQuery(viewquery, ar -> {
            JsonObject result = checkFine(ar);
            assertTrue(result.getJsonArray("result").size() > 1);
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
