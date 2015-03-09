package io.vertx.ext.couchbase;


import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.couchbase.impl.Stale;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by levin on 10/27/2014.
 */
public class CouchbaseServiceTestBase extends CouchbaseServiceVerticleTest {


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
        
        String id = UUID.randomUUID().toString();

        JsonObject command = new JsonObject();
        command.put("doctype", "user");
        command.put("id", id).put("content", new JsonObject().put("1", "2"));

        cbService.insert(command, i -> {
            cbService
                .findOne(command, ar -> {
                    if (ar.succeeded()) {
                        JsonObject result = ar.result();
                        if ("ok".equals(result.getString("status"))) {
                            JsonObject dbUser = result.getJsonObject("result");
                            assertEquals(dbUser.getString("1"), "2");
                            testComplete();
                        } else {
                            fail(result.getString("status"));
                        }
                    } else {
                        fail(ar.cause().getMessage());
                    }
                });
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


    public JsonObject genFixture() {
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
    public void genFixtures() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(100);
        for (int i = 0; i < 100; ++i) {
            cbService.insert(genFixture(), ar -> {
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
    public void testViewQueryKeys() {
        JsonObject insert = new JsonObject()
            .put("doctype", "test")
            .put("id", UUID.randomUUID().toString())
            .put("content", new JsonObject()
                .put("test1", "1")
                .put("test2", "2")
            );
        cbService.insert(insert, ar -> {
            JsonObject viewquery = new JsonObject()
                .put("design", "dev_test")
                .put("view", "test_keys")
                .put("keys", new JsonArray()
                    .add(new JsonArray().add("1").add("2"))
                );
            cbService.viewQuery(viewquery, ar2 -> {
                JsonObject result = checkFine(ar);
                assertTrue(result.getJsonArray("result").size() > 1);
                testComplete();
            });
        });
        await();
    }
    
    @Test
    public void testViewQueryStale() {
        // dont know how to test it...inspect var only
        JsonObject command = new JsonObject()
            .put("design", "dev_test")
            .put("view", "test_keys")
            .put("stale", Stale.FALSE.identifier());
        cbService.viewQuery(command, ar -> {
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

    @Test
    public void testUpdateNoSuchElement() {
        String id = UUID.randomUUID().toString();
        String doctype = "test";
        JsonObject command = new JsonObject()
            .put("id", id)
            .put("doctype", doctype)
            .put("update", new JsonObject()
                .put("$set", new JsonObject()
                    .put("name", "x")
                )
            );
        cbService.update(command, ar -> {
            if (ar.failed()) {
                assertTrue(ar.cause() instanceof Exception);
                testComplete();
            } else {
                fail("should fail");
            }
        });
        await();
    }

    @Test
    public void testBulk() {
        String doctype = "test";

        String id1 = UUID.randomUUID().toString();
        JsonObject insert1 =  new JsonObject()
            .put("_actionName", "insert")
            .put("doctype", doctype)
            .put("id", id1);

        String id2 = UUID.randomUUID().toString();
        JsonObject insert2 =  new JsonObject()
            .put("_actionName", "insert")
            .put("doctype", doctype)
            .put("id", id2);

        JsonObject command = new JsonObject()
            .put("actions", new JsonArray()
                .add(insert1)
                .add(insert2)
            );

        cbService.bulk(command, ar -> {
            JsonObject object = checkFine(ar);
            JsonArray results = object.getJsonArray("results");
            assertEquals(doctype + ":" + id1, results.getJsonObject(0).getString("id"));
            assertEquals(doctype + ":" + id2, results.getJsonObject(1).getString("id"));
        });

    }

}
