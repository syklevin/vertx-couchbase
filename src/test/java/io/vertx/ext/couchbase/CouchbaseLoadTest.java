package io.vertx.ext.couchbase;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import java.util.Date;
import java.util.UUID;

/**
 * Created by tommykwan on 26/2/15.
 */
public class CouchbaseLoadTest extends CouchbaseServiceVerticleTest {

    @Test
    public void testLoadInsert() throws Exception {
        long startTime = System.currentTimeMillis();
        final int successCounts[] = {0};
        final int errorCounts[] = {0};
        int count = 5000;
        for (int i = 0; i < count; ++i) {
            JsonObject obj = new JsonObject()
                .put("doctype", "doc")
                .put("id", UUID.randomUUID().toString() + "-" + i)
                .put("content", new JsonObject()
                        .put("time", (new Date()).toString())
                );
            cbService.insert(obj, ar -> {
                if (ar.succeeded()) {
                    String id = ar.result().getString("id");
                    System.out.println("done " + id + ": " + (System.currentTimeMillis() - startTime) + " ms");
                    successCounts[0]++;
                } else {
                    System.out.println("error: " + (System.currentTimeMillis() - startTime) + " ms");
                    errorCounts[0]++;
                }
                if (successCounts[0] + errorCounts[0] == count) {
                    System.out.println("success: " + successCounts[0] + ", error: " + errorCounts[0]);
                    testComplete();
                }
            });
        }
        await();
    }

    @Test
    public void testLoadUpsert() throws Exception {
        long startTime = System.currentTimeMillis();
        final int successCounts[] = {0};
        final int errorCounts[] = {0};
        int count = 5000;
        for (int i = 0; i < count; ++i) {
            JsonObject obj = new JsonObject()
                .put("doctype", "doc")
                .put("upsert", true)
                .put("id", i + "")
                .put("content", new JsonObject()
                        .put("time", (new Date()).toString())
                );
            cbService.insert(obj, ar -> {
                if (ar.succeeded()) {
                    String id = ar.result().getString("id");
                    System.out.println("done " + id + ": " + (System.currentTimeMillis() - startTime) + " ms");
                    successCounts[0]++;
                } else {
                    System.out.println("error: " + (System.currentTimeMillis() - startTime) + " ms");
                    errorCounts[0]++;
                }
                if (successCounts[0] + errorCounts[0] == count) {
                    System.out.println("success: " + successCounts[0] + ", error: " + errorCounts[0]);
                    testComplete();
                }
            });
        }
        await();
    }

    @Test
    public void testLoadFindOne() throws Exception {
        long startTime = System.currentTimeMillis();
        final int successCounts[] = {0};
        final int errorCounts[] = {0};
        int count = 5000;
        for (int i = 0; i < count; ++i) {
            JsonObject obj = new JsonObject()
                .put("doctype", "doc")
                .put("id", i + "");
            cbService.findOne(obj, ar -> {
                if (ar.succeeded()) {
                    String id = ar.result().getString("id");
                    System.out.println("done " + id + ": " + (System.currentTimeMillis() - startTime) + " ms");
                    successCounts[0]++;
                } else {
                    System.out.println("error: " + (System.currentTimeMillis() - startTime) + " ms");
                    errorCounts[0]++;
                }
                if (successCounts[0] + errorCounts[0] == count) {
                    System.out.println("success: " + successCounts[0] + ", error: " + errorCounts[0]);
                    testComplete();
                }
            });
        }
        await();
    }

    @Test
    public void testLoadBulkFindOne() throws Exception {
        long startTime = System.currentTimeMillis();
        int count = 5000;
        JsonArray actions = new JsonArray();
        for (int i = 0; i < count; ++i) {
            JsonObject obj = new JsonObject()
                .put("_actionName", "findOne")
                .put("doctype", "doc")
                .put("id", i + "");
            actions.add(obj);
        }
        JsonObject bulk = new JsonObject()
            .put("actions", actions);
        cbService.bulk(bulk, ar -> {
            if (ar.succeeded()) {
                System.out.println("done: " + (System.currentTimeMillis() - startTime) + " ms");
                testComplete();
            }
        });
        await();
    }

    @Test
    public void testLoadUpdateArray() throws Exception {
        long startTime = System.currentTimeMillis();
        final int successCounts[] = {0};
        final int errorCounts[] = {0};
        int count = 5000;
        String ID = UUID.randomUUID().toString();
        JsonObject subject = new JsonObject()
            .put("doctype", "doc")
            .put("upsert", true)
            .put("id", ID)
            .put("content", new JsonObject().put("fd_ids", new JsonArray()));
        cbService.insert(subject, a -> {
            for (int i = 0; i < count; ++i) {
                JsonObject obj = new JsonObject()
                    .put("doctype", "doc")
                    .put("id", ID)
                    .put("content", new JsonObject().put("time", (new Date()).toString()));
                cbService.update(obj, ar -> {
                    if (ar.succeeded()) {
                        String id = ar.result().getString("id");
                        Long cas = ar.result().getLong("cas");
                        System.out.println("done " + id + " - " + cas + " : " + (System.currentTimeMillis() - startTime) + " ms");
                        successCounts[0]++;
                    } else {
                        System.out.println("error: " + (System.currentTimeMillis() - startTime) + " ms");
                        errorCounts[0]++;
                    }
                    if (successCounts[0] + errorCounts[0] == count) {
                        System.out.println("success: " + successCounts[0] + ", error: " + errorCounts[0]);
                        JsonObject o = new JsonObject()
                            .put("doctype", "doc")
                            .put("id", ID);
                        cbService.findOne(o, gg -> {
                            assertEquals(count, gg.result().getJsonObject("result").getJsonArray("fd_ids").size());
                            testComplete();
                        });
                    }
                });
            }
        });
        await();
    }

    private JsonObject genRandomPlayerById(String id) {
        return new JsonObject()
            .put("id", id)
            .put("doctype", "player")
            .put("upsert", true)
            .put("content", new JsonObject()
                    .put("isOnline", false)
                    .put("tableId", 1)
            );
    }

    private JsonObject genPlayerRelationship(String id1, String id2) {
        return new JsonObject()
            .put("id", UUID.randomUUID().toString())
            .put("doctype", "pp")
            .put("upsert", true)
            .put("content", new JsonObject()
                    .put("player_id", id1)
                    .put("friend_id", id2)
            );
    }

    @Test
    public void testFriendSystem() {
        String playerId1 = UUID.randomUUID().toString();
        String playerId2 = UUID.randomUUID().toString();

        cbService.insert(genRandomPlayerById(playerId1), done1 -> {
            cbService.insert(genRandomPlayerById(playerId2), done2 -> {
                cbService.insert(genPlayerRelationship(playerId1, playerId2), done3 -> {
                    JsonObject viewQueryJson = new JsonObject()
                        .put("design", "dev_helios")
                        .put("view", "find_friends_by_player_id")
                        .put("reduce", true)
                        .put("keys", new JsonArray().add(playerId1));
                    cbService.viewQuery(viewQueryJson, done4 -> {
                        assertEquals(1, done4.result().getJsonArray("result").size());
                        testComplete();
                    });
                });
            });
        });

        await();
    }

}
