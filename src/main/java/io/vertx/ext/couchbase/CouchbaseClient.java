package io.vertx.ext.couchbase;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.ProxyIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

/**
 * Created by levin on 6/1/2015.
 */

@VertxGen
@ProxyGen
public interface CouchbaseClient {

    void findOne(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void insert(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void update(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void deleteOne(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void viewQuery(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void n1ql(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void dbInfo(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    void bulk(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler);

    @ProxyIgnore
    void start(Handler<AsyncResult<Void>> asyncHandler);

    @ProxyIgnore
    void stop(Handler<AsyncResult<Void>> asyncHandler);
}
