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


    protected JsonObject getConfig() {
        JsonObject couchbaseCfg = new JsonObject()
                .putString("bucket", "default")
                .putString("password", "");
        JsonArray nodes = new JsonArray();
        nodes.add("192.168.0.36");
        couchbaseCfg.putArray("nodes", nodes);
        return couchbaseCfg;
    }

    @Test
    public void testCreateAndGetCollection() throws Exception {

        JsonObject command = new JsonObject();
        command.putString("doctype", "user");
        command.putString("id", UUID.randomUUID().toString());

        JsonObject content = new JsonObject();

        content.putString("username", "peter");
        content.putString("password", "123456");
        content.putString("email", "peter@abc.com");
        command.putObject("content", content);

        cbService.insert(command, (result) -> {
            JsonObject jsonData = result.result();
            if("ok".equals(jsonData.getString("status"))){
                JsonObject dbUser = jsonData.getObject("result");
                assertEquals(dbUser.getString("username"), "peter");

                testComplete();
            }
            else{
                fail(jsonData.getString("status"));
            }
        });

        await();
    }




}
