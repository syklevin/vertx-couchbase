package io.vertx.ext.couchbase;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Created by levin on 10/27/2014.
 */
public class CouchbaseServiceTest extends VertxTestBase {


    @Override
    public void setUp() throws Exception {
        super.setUp();
//        JsonObject config = getConfig();
//        cbService = CouchbaseService.create(vertx, config);
//        cbService.start();
    }

    @Override
    public void tearDown() throws Exception {
        //cbService.stop();
        super.tearDown();
    }

}
