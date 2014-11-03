package io.vertx.ext.couchbase;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Created by levin on 10/27/2014.
 */
public class CouchbaseServiceTest extends CouchbaseServiceTestBase {


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
