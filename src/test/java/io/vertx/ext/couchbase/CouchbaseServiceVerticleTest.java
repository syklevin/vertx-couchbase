package io.vertx.ext.couchbase;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
* Created by levin on 1/19/2015.
*/
public class CouchbaseServiceVerticleTest extends CouchbaseServiceTestBase {

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


}
