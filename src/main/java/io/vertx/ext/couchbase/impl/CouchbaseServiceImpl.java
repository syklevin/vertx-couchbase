package io.vertx.ext.couchbase.impl;

import com.couchbase.client.java.*;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.transcoder.Transcoder;
import com.couchbase.client.java.view.*;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;
import io.vertx.ext.couchbase.CouchbaseService;
import io.vertx.ext.couchbase.impl.parser.CouchbaseUpdateJsonParser;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

/**
 * Created by levin on 10/27/2014.
 */
public class CouchbaseServiceImpl implements CouchbaseService {

    public static final Logger logger = LoggerFactory.getLogger(CouchbaseServiceImpl.class);
    public static final String DEFAULT_ADDRESS = "vertx.couchbase";
    private final Vertx vertx;
    private final JsonObject config;
    private final VertxScheduler vertxScheduler;

    private String address;
    private String bucketName;
    private int bucketPort;
    private CouchbaseEnvironment env;
    private AsyncCluster couchbase;
    private AsyncBucket bucket;
    private List<Transcoder<? extends Document, ?>> customTranscoders;
    private Boolean queryEnabled;

    public CouchbaseServiceImpl(Vertx vertx, JsonObject config) {
        this.vertx = vertx;
        this.config = config;
        this.vertxScheduler = new VertxScheduler(vertx);
        this.customTranscoders = new ArrayList<>();
        this.customTranscoders.add(new VertxJsonTranscoder());
    }

    @Override
    public void start(Handler<AsyncResult<Void>> asyncHandler) {
        address = config.getString("address", DEFAULT_ADDRESS);
        queryEnabled = config.getBoolean("queryEnabled", true);
        String bucketPwd = config.getString("password", "");
        bucketName = config.getString("bucket", "default");
        JsonArray nodesJsonArr = config.getJsonArray("nodes", new JsonArray());
        env = DefaultCouchbaseEnvironment.builder()
                .queryEnabled(queryEnabled)
                .build();

        couchbase = CouchbaseAsyncCluster.create(env, nodesJsonArr.getList());

        couchbase.openBucket(bucketName, bucketPwd, customTranscoders)
                .subscribe(asyncBucket -> {
                    bucket = asyncBucket;

                    if (queryEnabled) {

                        logger.info("create n1ql index for " + bucketName);
                        createN1qlIndex(ar -> {
                            if (ar.succeeded()) {
                                vertx.runOnContext(v -> asyncHandler.handle(Future.succeededFuture()));
                            } else {
                                vertx.runOnContext(v -> asyncHandler.handle(Future.failedFuture(ar.cause())));
                            }
                        });
                    } else {
                        vertx.runOnContext(v -> asyncHandler.handle(Future.succeededFuture()));
                    }

                }, e -> vertx.runOnContext(v -> asyncHandler.handle(Future.failedFuture(e))));
    }

    @Override
    public void stop(Handler<AsyncResult<Void>> asyncHandler) {
        if(couchbase != null){
            couchbase.disconnect()
                    .subscribe(aBoolean -> {
                        env.shutdown();
                        vertx.runOnContext(v -> asyncHandler.handle(Future.succeededFuture()));
                    }, e -> vertx.runOnContext(v -> asyncHandler.handle(Future.failedFuture(e))));
        }
        else{
            asyncHandler.handle(Future.succeededFuture());
        }
    }

    @Override
    public void findOne(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        doFindOne(command)
            .subscribe(jsonDoc -> {
                JsonObject rootNode = new JsonObject();
                rootNode.put("status", "ok");
                rootNode.put("id", jsonDoc.id());
                rootNode.put("cas", jsonDoc.cas());
                rootNode.put("result", new JsonObject(jsonDoc.content().toMap()));
                handleSuccessResult(rootNode, asyncHandler);
            }, e -> handleFailureResult(e, asyncHandler));
    }

    private Observable<JsonDocument> doFindOne(JsonObject command) {
        String doctype = command.getString("doctype");
        String id = command.getString("id");
        if(doctype == null || id == null) {
            return Observable.error(new Exception("doctype or id could not be null"));
        }
        final String docId = buildDocumentId(doctype, id);
        return bucket.get(docId).first();
    }

    @Override
    public void insert(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        doInsert(command)
          .flatMap((d) -> bucket.get(d.id(), VertxJsonDocument.class))
          .first()
          .subscribe(jsonDoc -> {
              JsonObject rootNode = new JsonObject();
              rootNode.put("status", "ok");
              rootNode.put("id", jsonDoc.id());
              rootNode.put("cas", jsonDoc.cas());
              rootNode.put("result", jsonDoc.content());
              handleSuccessResult(rootNode, asyncHandler);
          }, e -> handleFailureResult(e, asyncHandler));
    }

    private Observable<VertxJsonDocument> doInsert(JsonObject command) {
        String doctype = command.getString("doctype");
        String id = command.getString("id");
        if(doctype == null || id == null) {
            return Observable.error(new Exception("doctype or id could not be null"));
        }
        JsonObject content = command.getJsonObject("content");
        content.put("_id", content.getString("_id", id));
        content.put("_doctype", content.getString("_doctype", doctype));
        int expired = command.getInteger("expired", 0);
        long cas = command.getLong("cas", 0L);
        boolean upsert = command.getBoolean("upsert", false);
        String docId = buildDocumentId(doctype, id);

        VertxJsonDocument doc = VertxJsonDocument.create(docId, expired, content, cas);
        return upsert ? bucket.upsert(doc) :  bucket.insert(doc);
    }

    @Override
    public void update(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        Boolean upsert = command.getBoolean("upsert", false);
        JsonArray actions = command.getJsonArray("update", new JsonArray());
        Observable
            .defer(() -> doFindOne(command))
            .onErrorResumeNext(error -> {
                if (upsert && error instanceof NoSuchElementException) {
                    return Observable.just(null);
                } else {
                    return Observable.error(error);
                }
            })
            .flatMap(doc -> {
                JsonObject docJson = new JsonObject(doc.content().toMap());
                JsonObject newContent = CouchbaseUpdateJsonParser.updateJsonObject(docJson, actions);
                JsonObject newCommand = command.copy();
                newCommand
                    .put("upsert", true)
                    .put("cas", doc == null ? 0L : doc.cas())
                    .put("content", newContent);
                return doInsert(newCommand);
            })
            .retryWhen(attempts ->
                    attempts.flatMap(n -> {
                        if (!(n instanceof CASMismatchException)) {
                            return Observable.error(n);
                        }
                        return Observable.timer(100, TimeUnit.MILLISECONDS);
                    })
            )
            .subscribe(jsonDoc -> {
                JsonObject rootNode = new JsonObject();
                rootNode.put("status", "ok");
                rootNode.put("id", jsonDoc.id());
                rootNode.put("cas", jsonDoc.cas());
                rootNode.put("result", jsonDoc.content());
                handleSuccessResult(rootNode, asyncHandler);
            }, e -> handleFailureResult(e, asyncHandler));
    }

    public void deleteOne(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        String doctype = command.getString("doctype");
        String id = command.getString("id");
        String docId = buildDocumentId(doctype, id);
        bucket.remove(docId, VertxJsonDocument.class)
                .single(null)
                .subscribe(jsonDoc -> {
                    JsonObject rootNode = new JsonObject();
                    rootNode.put("status", "ok");
                    rootNode.put("id", jsonDoc.id());
                    rootNode.put("cas", jsonDoc.cas());
                    rootNode.put("result", jsonDoc.content());
                    handleSuccessResult(rootNode, asyncHandler);
                }, e -> handleFailureResult(e, asyncHandler));
    }

    public void n1ql(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler){
        String currBucketName = command.getString("bucket", bucketName);
        Query query = Query.simple(command.getString("query"));
        logger.info(query.toString());
        bucket.query(query)
                .flatMap(AsyncQueryResult::rows)
                //need to pluck result data from {"BucketName": { data }} -> { data }
                .flatMap(q -> Observable.just(new JsonObject(q.value().getObject(currBucketName).toMap())))
                .toList()
                .subscribe(list -> {
                    JsonObject rootNode = new JsonObject();
                    rootNode.put("status", "ok");
                    rootNode.put("total", list.size());
                    rootNode.put("result", new JsonArray(list));
                    handleSuccessResult(rootNode, asyncHandler);
                }, e -> handleFailureResult(e, asyncHandler));
    }

    protected void createN1qlIndex(Handler<AsyncResult<JsonObject>> asyncHandler){
        JsonObject command = new JsonObject().put("query", "CREATE PRIMARY INDEX ON " + bucketName);
        n1ql(command, asyncHandler);
    }

    public void viewQuery(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        String design = command.getString("design");

        String view = command.getString("view");

        if(design == null || design.length() == 0 || view == null || view.length() == 0){
            asyncHandler.handle(Future.failedFuture("design or view can not be null"));
            return;
        }

        ViewQuery viewQuery = ViewQuery.from(design, view);
        int skip = command.getInteger("skip", 0);
        int limit = command.getInteger("limit", 0);
        if(skip > 0)
            viewQuery.skip(skip);
        if(limit > 0)
            viewQuery.limit(limit);

        boolean group = command.getBoolean("group", false);
        int groupLeave = command.getInteger("groupLeave", 0);

        if(group){
            viewQuery.group(group);
            viewQuery.groupLevel(groupLeave);
        }
        String startKeyDocId = command.getString("startKeyDocId");
        if(startKeyDocId != null){
            viewQuery.startKeyDocId(startKeyDocId);
        }
        viewQuery.stale(Stale.FALSE);
        String endKeyDocId = command.getString("endKeyDocId");
        if(endKeyDocId != null){
            viewQuery.endKeyDocId(endKeyDocId);
        }
        JsonArray startKey = command.getJsonArray("startKey");
        if(startKey != null){
            viewQuery.startKey(com.couchbase.client.java.document.json.JsonArray.from(startKey.getList()));
        }
        JsonArray endKey = command.getJsonArray("endKey");
        if(endKey != null){
            viewQuery.startKey(com.couchbase.client.java.document.json.JsonArray.from(endKey.getList()));
        }
        JsonArray keys = command.getJsonArray("keys");
        if(keys != null){
            viewQuery.keys(com.couchbase.client.java.document.json.JsonArray.from(keys.getList()));
        }

        bucket.query(viewQuery)
            .flatMap(AsyncViewResult::rows)
                .flatMap(row -> row.document(VertxJsonDocument.class))
                .flatMap(doc -> Observable.just(doc.content()))
                .toList()
                .subscribe(list -> {
                    JsonObject rootNode = new JsonObject();
                    rootNode.put("status", "ok");
                    rootNode.put("result", new JsonArray(list));
                    handleSuccessResult(rootNode, asyncHandler);
                }, e -> handleFailureResult(e, asyncHandler));
    }

    public void dbInfo(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        bucket.bucketManager()
            .flatMap(asyncBucketManager -> asyncBucketManager.info())
            .subscribe(bucketInfo -> {
                    JsonObject rootNode = new JsonObject();
                    rootNode.put("status", "ok");
                    rootNode.put("name", bucketInfo.name());
                    rootNode.put("bucketType", bucketInfo.type().name());
                    rootNode.put("nodeCount", bucketInfo.nodeCount());
                    rootNode.put("replicaCount", bucketInfo.replicaCount());
                    handleSuccessResult(rootNode, asyncHandler);
                }, e -> handleFailureResult(e, asyncHandler));
    }

    protected void handleSuccessResult(JsonObject result, Handler<AsyncResult<JsonObject>> asyncHandler){
        vertx.runOnContext(v -> asyncHandler.handle(Future.succeededFuture(result)));
    }

    protected void handleFailureResult(Throwable throwable, Handler<AsyncResult<JsonObject>> asyncHandler){
        vertx.runOnContext(v -> asyncHandler.handle(Future.failedFuture(throwable)));
    }

    protected String buildDocumentId(String doctype, String id){
        return doctype + ":" + id;
    }
}
