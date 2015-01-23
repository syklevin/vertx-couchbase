package io.vertx.ext.couchbase;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
* Created by levin on 1/19/2015.
*/
public class CouchbaseServiceVerticleTest extends CouchbaseServiceTestBase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        String address = "vertx.couchbase";
        JsonObject config = getConfig();
        config.put("address", address);
        DeploymentOptions options = new DeploymentOptions(config);
        CountDownLatch latch = new CountDownLatch(1);
        vertx.deployVerticle("service:io.vertx:ext-couchbase", options, ar -> {
            if(ar.succeeded()) {
                System.out.println("success to deploy couchbase-service");
                cbService = CouchbaseService.createProxy(vertx, config);
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


}
