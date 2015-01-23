package io.vertx.ext.couchbase;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;

/**
 * Created by levin on 1/19/2015.
 */
public class CouchbaseServiceVerticle extends AbstractVerticle implements Handler<Message<JsonObject>> {

    CouchbaseService service;

    MessageConsumer consumer;

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        // And register it on the event bus against the configured address
        String address = config().getString("address");
        if (address == null) {
            startFuture.fail(new IllegalStateException("address field must be specified in config for service verticle"));
        }

        consumer = vertx.eventBus().consumer(address, this);
        service = CouchbaseService.create(vertx, config());

        // Start it
        service.start(ar -> {
            if (ar.succeeded()) {
                startFuture.complete();
            } else {
                startFuture.fail(ar.cause());
            }
        });
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        consumer.unregister();
        service.stop(ar -> {
            if (ar.succeeded()) {
                stopFuture.complete();
            } else {
                stopFuture.fail(ar.cause());
            }
        });
    }

    @Override
    public void handle(Message<JsonObject> message) {
        JsonObject command = message.body();
        String action = command.getString("action");

        Handler<AsyncResult<JsonObject>> handler = ar -> {
            if(ar.succeeded()){
                message.reply(ar.result());
            }
            else{
                JsonObject errorResponse = new JsonObject();
                errorResponse.put("status", "error");
                errorResponse.put("message", ar.cause().getMessage());
                message.reply(errorResponse);
            }
        };

        switch(action){
            case "findOne":
                service.findOne(command, handler);
                break;
            case "deleteOne":
                service.deleteOne(command, handler);
                break;
            case "insert":
            case "upsert":
                command.put("upsert", true);
                service.insert(command, handler);
                break;
            case "update":
                command.put("upsert", false);
                service.insert(command, handler);
                break;
            case "viewQuery":
                service.viewQuery(command, handler);
                break;
            case "n1ql":
                service.n1ql(command, handler);
                break;
            case "dbInfo":
                service.dbInfo(command, handler);
                break;
            default:
                handler.handle(Future.failedFuture(new Exception("Unkonwn action")));
        }
    }
}
