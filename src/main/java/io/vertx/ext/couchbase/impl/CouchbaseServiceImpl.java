package io.vertx.ext.couchbase.impl;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.AsyncBucketManager;
import com.couchbase.client.java.bucket.BucketInfo;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.transcoder.Transcoder;
import com.couchbase.client.java.view.*;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.couchbase.CouchbaseService;
import io.vertx.ext.rx.java.RxHelper;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Created by levin on 10/27/2014.
 */
public class CouchbaseServiceImpl implements CouchbaseService {

    public static final String DEFAULT_ADDRESS = "vertx.couchbase";

    private final Vertx vertx;
    private final JsonObject config;

    private String address;
    private Cluster couchbase;
    private Bucket bucket;

    public CouchbaseServiceImpl(Vertx vertx, JsonObject config) {
        this.vertx = vertx;
        this.config = config;
    }

    @Override
    public void start() {

        address = config.getString("address", DEFAULT_ADDRESS);
        String bucketPwd = config.getString("password", "");
        String bucketName = config.getString("bucket", "default");
//        JsonArray nodesJsonArr = config.getArray("nodes", new JsonArray().addString("localhost"));
//        List<String> couchNodes = nodesJsonArr.toList();
//        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
////                .queryEnabled(true)
//                .build();
//        couchbase = CouchbaseCluster.create(couchNodes);

        couchbase = CouchbaseCluster.create("127.0.0.1");

        List<Transcoder<? extends Document, ?>> customTranscoders = new ArrayList<>();
        customTranscoders.add(new VertxJsonTranscoder());
        bucket = couchbase.openBucket(bucketName, bucketPwd, customTranscoders);
    }

    @Override
    public void stop() {
        if(couchbase != null){
            couchbase.disconnect();
        }
    }

    @Override
    public void findOne(JsonObject command, Handler<AsyncResult<JsonObject>> resultHandler) {

    }

    public void insert(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler){

        String doctype = command.getString("doctype");
        String id = command.getString("id");
        JsonObject content = command.getObject("content");

        if (!content.containsField("_id")) {
            content.putString("_id", id);
        }
        if (!content.containsField("_doctype"))
            content.putString("_doctype", doctype);

        int expired = command.getInteger("expired", 0);
        long cas = command.getLong("cas", 0);
        boolean upsert = command.getBoolean("upsert", true);

        final String docId = buildDocumentId(doctype, id);
        final VertxJsonDocument doc = VertxJsonDocument.create(docId, expired, content, cas);

        Observable<VertxJsonDocument> o = upsert ? bucket.async().upsert(doc) :
                                                    bucket.async().insert(doc);

        o.flatMap((d) -> bucket.async().get(docId, VertxJsonDocument.class))
          .single()
          .subscribe((vertxJsonDocument) -> {
                      JsonObject rootNode = new JsonObject();
                      if (vertxJsonDocument == null) {
                          rootNode.putString("status", "error");
                      } else {
                          rootNode.putString("status", "ok");
                          rootNode.putString("id", vertxJsonDocument.id());
                          rootNode.putNumber("cas", vertxJsonDocument.cas());
                          rootNode.putObject("result", vertxJsonDocument.content());
                      }

                      vertx.context().runOnContext(ignored -> {
                          asyncHandler.handle(Future.completedFuture(rootNode));
                      });

                  },
                  (t) -> {
                      JsonObject rootNode = new JsonObject();
                      rootNode.putString("status", "error");
                      rootNode.putString("reason", t.getMessage());

                      vertx.context().runOnContext(ignored -> {
                          asyncHandler.handle(Future.completedFuture(rootNode));
                      });


                  });
    }

    public void delete(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler){

        String doctype = command.getString("doctype");
        String id = command.getString("id");

        final String docId = buildDocumentId(doctype, id);

        bucket.async().remove(docId, VertxJsonDocument.class)
                .singleOrDefault(null)
                .subscribe(new Action1<VertxJsonDocument>() {
                    @Override
                    public void call(VertxJsonDocument vertxJsonDocument) {
                        JsonObject rootNode = new JsonObject();
                        if (vertxJsonDocument == null) {
                            rootNode.putString("status", "error");
                            rootNode.putString("reason", "record not found");
                        }
                        else{
                            rootNode.putString("status", "ok");
                            rootNode.putString("id", vertxJsonDocument.id());
                            rootNode.putNumber("cas", vertxJsonDocument.cas());
                            rootNode.putObject("result", vertxJsonDocument.content());
                        }

                        vertx.context().runOnContext(ignored -> {
                            asyncHandler.handle(Future.completedFuture(rootNode));
                        });
                    }
                }, new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        JsonObject rootNode = new JsonObject();
                        rootNode.putString("status", "error");
                        rootNode.putString("reason", throwable.getMessage());

                        vertx.context().runOnContext(ignored -> {
                            asyncHandler.handle(Future.completedFuture(rootNode));
                        });
                    }
                });
    }

    public void viewQuery(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler){
        String design = command.getString("design");
        String view = command.getString("view");
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

        JsonArray startKey = command.getArray("startKey");

        if(startKey != null){
            com.couchbase.client.java.document.json.JsonArray startKeyCouchJsonArray = com.couchbase.client.java.document.json.JsonArray.empty();
            String key;
            for(int i=0; i<startKey.size(); i++){
                key = startKey.get(i);
                startKeyCouchJsonArray.add(key);
            }
            viewQuery.startKey(startKeyCouchJsonArray);
        }

        JsonArray endKey = command.getArray("endKey");

        if(endKey != null){
            com.couchbase.client.java.document.json.JsonArray endKeyCouchJsonArray = com.couchbase.client.java.document.json.JsonArray.empty();
            String key;
            for(int i=0; i<startKey.size(); i++){
                key = startKey.get(i);
                endKeyCouchJsonArray.add(key);
            }
            viewQuery.startKey(endKeyCouchJsonArray);
        }

        JsonArray keys = command.getArray("keys");

        if(keys != null){
            com.couchbase.client.java.document.json.JsonArray couchJsonArray = com.couchbase.client.java.document.json.JsonArray.empty();
            String key;
            for(int i=0; i<keys.size(); i++){
                key = keys.get(i);
                couchJsonArray.add(key);
            }
            viewQuery.keys(couchJsonArray);
        }

        JsonArray results = new JsonArray();


        bucket
            .async()
            .query(viewQuery)
            .flatMap(AsyncViewResult::rows)
            .flatMap((doc) -> doc.document(VertxJsonDocument.class))
            .subscribe(new Action1<VertxJsonDocument>() {
                @Override
                public void call(VertxJsonDocument vertxJsonDocument) {
                    results.add(vertxJsonDocument.content());
                }
            }, new Action1<Throwable>() {
                @Override
                public void call(Throwable throwable) {
                    JsonObject rootNode = new JsonObject();
                    rootNode.putString("status", "error");
                    rootNode.putString("reason", throwable.getMessage());

                    vertx.context().runOnContext(ignored -> {
                        asyncHandler.handle(Future.completedFuture(rootNode));
                    });

                }
            }, new Action0() {
                @Override
                public void call() {
                    JsonObject rootNode = new JsonObject();
                    rootNode.putString("status", "ok");
                    rootNode.putArray("result", results);

                    vertx.context().runOnContext(ignored -> {
                        asyncHandler.handle(Future.completedFuture(rootNode));
                    });
                }
            });

        //viewQuery.keys()

    }

    public void dbInfo(JsonObject command, Handler<AsyncResult<JsonObject>> asyncHandler){

        bucket.async().bucketManager()
            .flatMap(new Func1<AsyncBucketManager, Observable<BucketInfo>>() {
                @Override
                public Observable<BucketInfo> call(AsyncBucketManager asyncBucketManager) {
                    return asyncBucketManager.info();
                }
            })
            .subscribe(new Action1<BucketInfo>() {
            @Override
            public void call(BucketInfo bucketInfo) {
                JsonObject rootNode = new JsonObject();
                if (bucketInfo == null) {
                    rootNode.putString("status", "error");
                    rootNode.putString("reason", "not_found");
                } else {
                    rootNode.putString("status", "ok");
                    rootNode.putString("name", bucketInfo.name());
                    rootNode.putString("bucketType", bucketInfo.type().name());
                    rootNode.putNumber("nodeCount", bucketInfo.nodeCount());
                    rootNode.putNumber("replicaCount", bucketInfo.replicaCount());
                }
                vertx.context().runOnContext(ignored -> {
                    asyncHandler.handle(Future.completedFuture(rootNode));
                });
            }
        }, new Action1<Throwable>() {
            @Override
            public void call(Throwable throwable) {
                JsonObject rootNode = new JsonObject();
                rootNode.putString("status", "error");
                rootNode.putString("reason", throwable.getMessage());
                vertx.context().runOnContext(ignored -> {
                    asyncHandler.handle(Future.completedFuture(rootNode));
                });
            }
        });
    }

    private <T, U> void asHandler(Observable<T> observable, Handler<AsyncResult<U>> resultHandler, Function<T, U> converter) {
        Context context = vertx.context();

        observable.subscribe(new Action1<T>() {
            @Override
            public void call(T t) {
                resultHandler.handle(Future.completedFuture(converter.apply(t)));
            }
        }, new Action1<Throwable>() {
            @Override
            public void call(Throwable throwable) {
                resultHandler.handle(Future.completedFuture(throwable));
            }
        });
    }

    private String buildDocumentId(String doctype, String id){
        return doctype + ":" + id;
    }
}
