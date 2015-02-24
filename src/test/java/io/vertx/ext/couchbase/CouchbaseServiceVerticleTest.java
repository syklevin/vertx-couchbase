package io.vertx.ext.couchbase;

import io.vertx.core.AsyncResult;
import io.vertx.core.json.JsonObject;
import io.vertx.test.core.VertxTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
* Created by levin on 1/19/2015.
*/
public class CouchbaseServiceVerticleTest extends VertxTestBase {

    protected CouchbaseService cbService;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        CountDownLatch latch = new CountDownLatch(1);
        vertx.deployVerticle("service:io.vertx:ext-couchbase", ar -> {
            if(ar.succeeded()) {
                System.out.println("success to deploy couchbase-service");
                cbService = CouchbaseService.createProxy(vertx, "vertx.couchbase");
                System.out.println("success to create service proxy");
            }
            else{
                System.out.println("failed to deploy couchbase-service");
                System.out.println(ar.cause().getMessage());
            }
            latch.countDown();
        });
        latch.await();
    }

    @Test
    public void testVertxAsync() throws Exception {
        vertx.runOnContext(ctx -> {
            try {
                System.out.println("start wait for 500 millis");
                Thread.sleep(500);
                System.out.println("finish wait for 500 millis");
                Assert.assertTrue(true);
                testComplete();
            } catch (Exception ex) {
            }
        });

        await();
    }

    protected void checkOk(AsyncResult<JsonObject> result) {
        if (!result.succeeded())
            fail(result.cause().getMessage());
    }

    protected void checkStatus(JsonObject result) {
        if (!"ok".equals(result.getString("status")))
            fail(result.getString("status"));
    }

    protected JsonObject checkFine(AsyncResult<JsonObject> ar) {
        checkOk(ar);
        JsonObject result = ar.result();
        checkStatus(result);
        return result;
    }


}
