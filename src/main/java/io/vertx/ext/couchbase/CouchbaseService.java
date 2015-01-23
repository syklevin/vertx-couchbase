package io.vertx.ext.couchbase;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.couchbase.impl.CouchbaseServiceImpl;
import io.vertx.ext.couchbase.impl.CouchbaseServiceProxy;


/**
 * Created by levin on 10/27/2014.
 */
public interface CouchbaseService {

    static CouchbaseService create(Vertx vertx, JsonObject config) {
        return new CouchbaseServiceImpl(vertx, config);
    }

    static CouchbaseService createProxy(Vertx vertx, JsonObject config) {
        return new CouchbaseServiceProxy(vertx, config);
    }

    void findOne(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void insert(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void deleteOne(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void viewQuery(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void n1ql(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void dbInfo(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void start(Handler<AsyncResult<Void>> asyncHandler);

    void stop(Handler<AsyncResult<Void>> asyncHandler);
}
