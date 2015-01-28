package io.vertx.ext.couchbase;

import io.vertx.core.json.JsonObject;
import java.util.concurrent.CountDownLatch;


/**
 * Created by levin on 10/27/2014.
 */
public class CouchbaseServiceTest extends CouchbaseServiceTestBase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        JsonObject config = getConfig();
        cbService = CouchbaseService.create(vertx, config);
        CountDownLatch latch = new CountDownLatch(1);
        cbService.start(ar -> {
            if(ar.succeeded()){
                System.out.println("success to create CouchbaseService");
            }
            else{
                System.out.println("failed to create CouchbaseService");
                System.out.println(ar.cause().getMessage());
            }
            latch.countDown();
        });
        latch.await();
    }


}
