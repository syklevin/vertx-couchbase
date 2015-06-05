package io.vertx.ext.couchbase.impl;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.CouchbaseAsyncCluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.query.AsyncQueryResult;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.transcoder.Transcoder;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.AsyncViewRow;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import io.vertx.ext.couchbase.CouchbaseService;
import io.vertx.ext.couchbase.impl.parser.UpdateJsonParser;
import io.vertx.ext.couchbase.impl.parser.ViewQueryKeyParser;
import org.apache.logging.log4j.MarkerManager;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
/**
 * Created by levin on 10/27/2014.
 */
public class CouchbaseServiceImpl implements CouchbaseService {

    public static final Logger logger = LogManager.getLogger(CouchbaseServiceImpl.class);
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
                rootNode.put("result", jsonDoc.content());
                handleSuccessResult(rootNode, asyncHandler);
            }, e -> handleFailureResult(e, asyncHandler));
    }

    private Observable<VertxJsonDocument> doFindOne(JsonObject command) {
        String doctype = command.getString("doctype");
        String id = command.getString("id");
        if(doctype == null || id == null) {
            return Observable.error(new Exception("doctype or id could not be null"));
        }
        final String docId = buildDocumentId(doctype, id);
        return bucket.get(docId, VertxJsonDocument.class).first();
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
        JsonObject content = command.getJsonObject("content", new JsonObject());
        content.put("_id", content.getString("_id", id));
        content.put("_doctype", content.getString("_doctype", doctype));
        int expired = command.getInteger("expired", 0);
        long cas = command.getLong("cas", 0L);
        boolean upsert = command.getBoolean("upsert", false);
        String docId = buildDocumentId(doctype, id);

        VertxJsonDocument doc = VertxJsonDocument.create(docId, expired, content, cas);
        return upsert ? bucket.upsert(doc) :  bucket.insert(doc);
    }

    private Observable<VertxJsonDocument> doUpdate(JsonObject command) {
        Boolean upsert = command.getBoolean("upsert", false);
        JsonObject update = command.getJsonObject("update");
        if (update == null || update.size() == 0) {
            return Observable.error(new Exception("invalid update content"));
        }
        int maxTries = command.getInteger("maxTries", 10);
        final int[] tries = {0};
        return Observable
            .defer(() -> doFindOne(command))
            .onErrorResumeNext(error -> {
                // no such element create one
                if (upsert && error instanceof NoSuchElementException) {
                    return Observable.just(null);
                } else {
                    return Observable.error(error);
                }
            })
            .flatMap(doc -> {
                JsonObject content = new JsonObject();
                if (doc != null) {
                    content = doc.content().copy();
                }
                JsonObject newContent = UpdateJsonParser.updateJsonObject(content, update);
                JsonObject newCommand = command.copy()
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
                        if (++tries[0] > maxTries) {
                            return Observable.error(n);
                        }
                        return Observable.timer(100, TimeUnit.MILLISECONDS);
                    })
            );
    }

    @Override
    public void update(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        doUpdate(command)
            .subscribe(jsonDoc -> {
                JsonObject rootNode = new JsonObject();
                rootNode.put("status", "ok");
                rootNode.put("id", jsonDoc.id());
                rootNode.put("cas", jsonDoc.cas());
                rootNode.put("result", jsonDoc.content());
                handleSuccessResult(rootNode, asyncHandler);
            }, e -> handleFailureResult(e, asyncHandler));
    }

    private Observable<JsonDocument> doDeleteOne(JsonObject command) {
        String doctype = command.getString("doctype");
        String id = command.getString("id");
        String docId = buildDocumentId(doctype, id);

        return bucket.remove(docId);
    }

    public void deleteOne(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        doDeleteOne(command)
                .subscribe(jsonDoc -> {
                    JsonObject rootNode = new JsonObject();
                    rootNode.put("status", "ok");
                    handleSuccessResult(rootNode, asyncHandler);
                }, e -> handleFailureResult(e, asyncHandler));
    }

    private Observable<AsyncQueryResult> doN1ql(JsonObject command) {
        Query query = Query.simple(command.getString("query"));
        return bucket.query(query);
    }

    public void n1ql(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler){
        String currBucketName = command.getString("bucket", bucketName);
        doN1ql(command)
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

    private Observable<AsyncViewResult> doViewQuery(JsonObject command) {
        String design = command.getString("design");

        String view = command.getString("view");

        if (design == null || design.length() == 0 || view == null || view.length() == 0) {
            return Observable.error(new Exception("design or view can not be null"));
        }

        ViewQuery viewQuery = ViewQuery.from(design, view);
        int skip = command.getInteger("skip", 0);
        int limit = command.getInteger("limit", 0);
        if (skip > 0) viewQuery.skip(skip);
        if (limit > 0) viewQuery.limit(limit);

        boolean group = command.getBoolean("group", false);
        int groupLevel = command.getInteger("groupLevel", 0);

        if (group) viewQuery.group(group);
        if (groupLevel > 0) viewQuery.groupLevel(groupLevel);

        boolean descending = command.getBoolean("descending", false);
        if (descending) viewQuery.descending(descending);

        String startKeyDocId = command.getString("startKeyDocId");
        if (startKeyDocId != null) {
            viewQuery.startKeyDocId(startKeyDocId);
        }
        
        String stale = command.getString("stale", io.vertx.ext.couchbase.impl.Stale.FALSE.identifier());
        viewQuery.stale(io.vertx.ext.couchbase.impl.Stale.toStale(stale));
        
        String endKeyDocId = command.getString("endKeyDocId");
        if (endKeyDocId != null) {
            viewQuery.endKeyDocId(endKeyDocId);
        }
        Object startKey = command.getValue("startKey");
        if (startKey != null) {
            try {
                ViewQueryKeyParser.parseStartKey(viewQuery, startKey);
            } catch (Exception e) {
                return Observable.error(e);
            }
        }
        Object endKey = command.getValue("endKey");
        if (endKey != null) {
            try {
                ViewQueryKeyParser.parseEndKey(viewQuery, endKey);
            } catch (Exception e) {
                return Observable.error(e);
            }
        }
        Object key = command.getValue("key");
        if (key != null) {
            try {
                ViewQueryKeyParser.parseKey(viewQuery, key);
            } catch (Exception e) {
                return Observable.error(e);
            }
        }
        JsonArray keys = command.getJsonArray("keys");
        if (keys != null) {
            com.couchbase.client.java.document.json.JsonArray objects = com.couchbase.client.java.document.json.JsonArray.create();
            for (int i = 0; i < keys.size(); i++) {
                Object value = keys.getValue(i);
                if (value instanceof JsonArray) {
                    objects.add(((JsonArray) value).getList());
                } else {
                    try {
                        objects.add(value);
                    } catch (IllegalArgumentException ex) {
                        return Observable.error(new Exception("invalid key format"));
                    }
                }
            }
            viewQuery.keys(objects);
        }
        Boolean reduce = command.getBoolean("reduce", false);
        if (reduce) {
            viewQuery.reduce(reduce);
        }

        return bucket.query(viewQuery);
    }

    public void viewQuery(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {

        Observable<AsyncViewResult> asyncViewResultObservable = doViewQuery(command);
        Observable<AsyncViewRow> asyncViewRowObservable = asyncViewResultObservable.flatMap(AsyncViewResult::rows);
        Observable<JsonObject> docObservable;

        Boolean reduce = command.getBoolean("reduce", false);
        if (reduce) {
            docObservable = asyncViewRowObservable
                .concatMap(row -> {
                    // NOTE: no id can't parse to VertxDoc
                    JsonObject rowJson = new JsonObject();
                    Object key = row.key();
                    if (key instanceof com.couchbase.client.java.document.json.JsonArray) {
                        com.couchbase.client.java.document.json.JsonArray cbJsonArray = (com.couchbase.client.java.document.json.JsonArray) key;
                        rowJson.put("key", new JsonArray(cbJsonArray.toList()));
                        //rowJson.put("key", new JsonArray(key.toString()));
                    } else {
                        rowJson.put("key", key);
                    }
                    Object value = row.value();
                    if (value instanceof com.couchbase.client.java.document.json.JsonArray) {
                        com.couchbase.client.java.document.json.JsonArray cbJsonArray = (com.couchbase.client.java.document.json.JsonArray) value;
                        rowJson.put("value", new JsonArray(cbJsonArray.toList()));
                    } else {
                        rowJson.put("value", value);
                    }
                    return Observable.just(rowJson);
                });
        } else {
            docObservable = asyncViewRowObservable
                .concatMap(row -> row.document(VertxJsonDocument.class))
                .concatMap(doc -> Observable.just(doc.content()));
        }

        docObservable
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

    @Override
    public void bulk(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        JsonArray actionsJsonArray = command.getJsonArray("actions");
        if (actionsJsonArray == null) {
            asyncHandler.handle(Future.failedFuture("missing actions key"));
            return;
        }
        List<JsonObject> actions = actionsJsonArray.getList();
        Observable
            .from(actions)
            .flatMap(jsonObject -> {
                switch (jsonObject.getString("_actionName")) {
                    case "insert":
                        return doInsert(jsonObject);
                    case "update":
                        return doUpdate(jsonObject);
                    case "findOne":
                        return doFindOne(jsonObject);
                    case "deleteOne":
                        return doDeleteOne(jsonObject);
                    default:
                        return Observable.empty();
                }
            })
            .toList()
            .subscribe(list -> {
                JsonObject ret = statusOk();
                JsonArray results = new JsonArray();
                for (int i = 0; i < list.size(); i++) {
                    JsonObject jsonObject = parseResultFromSubscribe(list.get(i));
                    results.add(jsonObject);
                }
                ret.put("results", results);
                handleSuccessResult(ret, asyncHandler);
            }, e -> handleFailureResult(e, asyncHandler));
    }

    protected JsonObject statusOk() {
        JsonObject ret = new JsonObject()
            .put("status", "ok");
        return ret;
    }

    protected JsonObject parseResultFromSubscribe(Object result) {
        JsonObject ret = new JsonObject();
        if (result instanceof List) {
            ret.put("result", new JsonArray((List)result));
        } else if (result instanceof VertxJsonDocument) {
            VertxJsonDocument doc = (VertxJsonDocument) result;
            ret.put("result", doc.content())
                .put("cas", doc.cas())
                .put("id", doc.id());
        }
        return ret;
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
