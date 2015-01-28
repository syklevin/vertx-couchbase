package io.vertx.ext.couchbase;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.ProxyIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.couchbase.impl.CouchbaseServiceImpl;
import io.vertx.serviceproxy.ProxyHelper;


/**
 * Created by levin on 10/27/2014.
 */
@VertxGen
@ProxyGen
public interface CouchbaseService {

    static CouchbaseService create(Vertx vertx, JsonObject config) {
        return new CouchbaseServiceImpl(vertx, config);
    }

    static CouchbaseService createProxy(Vertx vertx, String address) {
        return ProxyHelper.createProxy(CouchbaseService.class, vertx, address);
//        return new CouchbaseServiceProxy(vertx, config);
    }

    void findOne(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void insert(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void deleteOne(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void viewQuery(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void n1ql(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void dbInfo(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    @ProxyIgnore
    void start(Handler<AsyncResult<Void>> asyncHandler);

    @ProxyIgnore
    void stop(Handler<AsyncResult<Void>> asyncHandler);
}
