package io.vertx.ext.couchbase.impl;

import com.couchbase.client.java.*;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.transcoder.Transcoder;
import com.couchbase.client.java.view.*;
import static com.couchbase.client.java.query.Select.*;
import static com.couchbase.client.java.query.dsl.Expression.*;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;
import io.vertx.ext.couchbase.CouchbaseService;
import rx.Observable;

import javax.naming.OperationNotSupportedException;
import java.util.ArrayList;
import java.util.List;

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
    private CouchbaseEnvironment env;
    private AsyncCluster couchbase;
    private AsyncBucket bucket;
    private List<Transcoder<? extends Document, ?>> customTranscoders;
    private Boolean queryEnabled;

    public CouchbaseServiceImpl(Vertx vertx, JsonObject config) {
        this.vertx = vertx;
        this.config = config;
        this.vertxScheduler = new VertxScheduler(vertx);
        //this.customTranscoders = new ArrayList<>();
        //this.customTranscoders.add(new VertxJsonTranscoder());
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
        couchbase.openBucket(bucketName, bucketPwd)
                .subscribe(asyncBucket -> {
                    bucket = asyncBucket;

                    if(queryEnabled){

                        logger.info("create n1ql index for " + bucketName);
                        createN1qlIndex(ar -> {
                            if(ar.succeeded()){
                                vertx.runOnContext(v -> asyncHandler.handle(Future.succeededFuture()));
                            }
                            else{
                                vertx.runOnContext(v -> asyncHandler.handle(Future.failedFuture(ar.cause())));
                            }
                        });
                    }
                    else{
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
        String doctype = command.getString("doctype");
        String id = command.getString("id");
        if(doctype == null || id == null) {
            asyncHandler.handle(Future.failedFuture(new Exception("doctype or id could not be null")));
            return;
        }
        final String docId = buildDocumentId(doctype, id);
        bucket.get(docId)
                .single()
                .subscribe(jsonDoc -> {
                    JsonObject rootNode = new JsonObject();
                    rootNode.put("status", "ok");
                    rootNode.put("id", jsonDoc.id());
                    rootNode.put("cas", jsonDoc.cas());
                    rootNode.put("result", new JsonObject(jsonDoc.content().toMap()));
                    handleSuccessResult(rootNode, asyncHandler);
                }, e -> handleFailureResult(e, asyncHandler));
    }


    @Override
    public void insert(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        String doctype = command.getString("doctype");
        String id = command.getString("id");
        JsonObject content = command.getJsonObject("content");
        if (!content.containsKey("_id")) {
            content.put("_id", id);
        }
        if (!content.containsKey("_doctype"))
            content.put("_doctype", doctype);
        int expired = command.getInteger("expired", 0);
        long cas = command.getLong("cas", 0L);
        boolean upsert = command.getBoolean("upsert", false);
        final String docId = buildDocumentId(doctype, id);
        //final VertxJsonDocument doc = VertxJsonDocument.create(docId, expired, content, cas);

        //translate from vertx jsonObject to couchbase jsonObject
        com.couchbase.client.java.document.json.JsonObject cbJson =
                com.couchbase.client.java.document.json.JsonObject.from(content.getMap());

        JsonDocument doc = JsonDocument.create(docId, expired, cbJson, cas);

        Observable<JsonDocument> o = upsert ? bucket.upsert(doc) :  bucket.insert(doc);
        o.flatMap((d) -> bucket.get(docId))
          .single()
          .subscribe(jsonDoc -> {
              JsonObject rootNode = new JsonObject();
              rootNode.put("status", "ok");
              rootNode.put("id", jsonDoc.id());
              rootNode.put("cas", jsonDoc.cas());
              rootNode.put("result", new JsonObject(jsonDoc.content().toMap()));
              handleSuccessResult(rootNode, asyncHandler);
          }, e -> handleFailureResult(e, asyncHandler));
    }

    public void deleteOne(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler) {
        String doctype = command.getString("doctype");
        String id = command.getString("id");
        String docId = buildDocumentId(doctype, id);
        bucket.remove(docId)
                .single(null)
                .subscribe(jsonDoc -> {
                    JsonObject rootNode = new JsonObject();
                    rootNode.put("status", "ok");
                    rootNode.put("id", jsonDoc.id());
                    rootNode.put("cas", jsonDoc.cas());
                    rootNode.put("result", new JsonObject(jsonDoc.content().toMap()));
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
            com.couchbase.client.java.document.json.JsonArray startKeyCouchJsonArray =
                    com.couchbase.client.java.document.json.JsonArray.empty();
            String key;
            for(int i=0; i<startKey.size(); i++){
                key = startKey.getString(i);
                startKeyCouchJsonArray.add(key);
            }
            viewQuery.startKey(startKeyCouchJsonArray);
        }
        JsonArray endKey = command.getJsonArray("endKey");
        if(endKey != null){
            com.couchbase.client.java.document.json.JsonArray endKeyCouchJsonArray = com.couchbase.client.java.document.json.JsonArray.empty();
            String key;
            for(int i=0; i<startKey.size(); i++){
                key = startKey.getString(i);
                endKeyCouchJsonArray.add(key);
            }
            viewQuery.startKey(endKeyCouchJsonArray);
        }
        JsonArray keys = command.getJsonArray("keys");
        if(keys != null){
            com.couchbase.client.java.document.json.JsonArray couchJsonArray = com.couchbase.client.java.document.json.JsonArray.empty();
            String key;
            for(int i=0; i<keys.size(); i++){
                key = keys.getString(i);
                couchJsonArray.add(key);
            }
            viewQuery.keys(couchJsonArray);
        }

        bucket.query(viewQuery)
            .flatMap(AsyncViewResult::rows)
                .flatMap(row -> row.document())
                .flatMap(doc -> Observable.just(new JsonObject(doc.content().toMap())))
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
