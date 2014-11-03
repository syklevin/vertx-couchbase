package io.vertx.ext.couchbase;


import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.test.core.TestUtils;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

import java.util.UUID;

/**
 * Created by levin on 10/27/2014.
 */
public class CouchbaseServiceTestBase extends VertxTestBase {


    protected CouchbaseService cbService;

    protected JsonObject getConfig() {
        JsonObject couchbaseCfg = new JsonObject()
                .putString("bucket", "beer-sample")
                .putString("password", "");
        JsonArray nodes = new JsonArray();
        nodes.add("192.168.0.87");
        couchbaseCfg.putArray("nodes", nodes);
        return couchbaseCfg;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        JsonObject config = getConfig();
        cbService = CouchbaseService.create(vertx, config);
        cbService.start();
    }

    @Override
    public void tearDown() throws Exception {
        cbService.stop();
        super.tearDown();
    }




}
