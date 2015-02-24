package io.vertx.ext.couchbase.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.couchbase.CouchbaseService;
import rx.Observable;


/**
 * Created by levin on 1/22/2015.
 */
public class CouchbaseServiceProxy implements CouchbaseService {

    private final Vertx vertx;
    private final DeliveryOptions ebOptions;
    private final String address;

    public CouchbaseServiceProxy(Vertx vertx, JsonObject config){
        this.vertx = vertx;
        this.address = config.getString("address");
        this.ebOptions = new DeliveryOptions();
        this.ebOptions.setSendTimeout(config.getLong("timeout", 30000L));
    }

    protected void processRequest(String action, JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler){
        command.put("action", action);
            this.vertx.eventBus().<JsonObject>send(address, command, ebOptions, ar -> {
                if (ar.succeeded()) {
                    asyncHandler.handle(Future.succeededFuture(ar.result().body()));
                } else {
                    asyncHandler.handle(Future.failedFuture(ar.cause()));
                }
            });
    }


    @Override
    public void findOne(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        processRequest("findOne", command, asyncHandler);
    }

    @Override
    public void insert(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        processRequest("insert", command, asyncHandler);
    }

    @Override
    public void update(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        processRequest("update", command, asyncHandler);
    }

    @Override
    public void deleteOne(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        processRequest("deleteOne", command, asyncHandler);
    }

    @Override
    public void viewQuery(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        processRequest("viewQuery", command, asyncHandler);
    }

    @Override
    public void n1ql(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        processRequest("n1ql", command, asyncHandler);
    }

    @Override
    public void dbInfo(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        processRequest("dbInfo", command, asyncHandler);
    }

    @Override
    public void bulk(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        processRequest("bulk", command, asyncHandler);
    }

    @Override
    public void start(Handler<AsyncResult<Void>> asyncHandler) {
        asyncHandler.handle(Future.succeededFuture());
    }

    @Override
    public void stop(Handler<AsyncResult<Void>> asyncHandler) {
        asyncHandler.handle(Future.succeededFuture());
    }
}
