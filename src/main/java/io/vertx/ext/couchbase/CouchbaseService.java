package io.vertx.ext.couchbase;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.ProxyIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.couchbase.impl.CouchbaseServiceImpl;
import io.vertx.proxygen.ProxyHelper;


import java.util.List;

/**
 * Created by levin on 10/27/2014.
 */
@VertxGen
@ProxyGen
public interface CouchbaseService {

    static CouchbaseService create(Vertx vertx, JsonObject config) {
        return new CouchbaseServiceImpl(vertx, config);
    }

    static CouchbaseService createEventBusProxy(Vertx vertx, String address) {
        return ProxyHelper.createProxy(CouchbaseService.class, vertx, address);
    }

    void insert(JsonObject command, Handler<AsyncResult<JsonObject>> resultHandler);

    void delete(JsonObject command, Handler<AsyncResult<JsonObject>> resultHandler);

    void viewQuery(JsonObject command, Handler<AsyncResult<JsonObject>> resultHandler);

    void dbInfo(JsonObject command, Handler<AsyncResult<JsonObject>> resultHandler);

    @ProxyIgnore
    void start();

    @ProxyIgnore
    void stop();
}
