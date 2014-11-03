package io.vertx.ext.couchbase.impl;

import com.couchbase.client.java.document.AbstractDocument;
import io.vertx.core.json.JsonObject;

/**
 * Created by levin on 8/27/2014.
 */
public class VertxJsonDocument extends AbstractDocument<JsonObject> {


    /**
     * Creates a empty {@link io.vertx.ext.couchbase.impl.VertxJsonDocument}.
     *
     * @return a empty {@link io.vertx.ext.couchbase.impl.VertxJsonDocument}.
     */
    public static VertxJsonDocument empty() {
        return new VertxJsonDocument(null, 0, null, 0);
    }

    /**
     * Creates a {@link io.vertx.ext.couchbase.impl.VertxJsonDocument} which the document id.
     *
     * @param id the per-bucket unique document id.
     * @return a {@link io.vertx.ext.couchbase.impl.VertxJsonDocument}.
     */
    public static VertxJsonDocument create(String id) {
        return new VertxJsonDocument(id, 0, null, 0);
    }

    /**
     * Creates a {@link io.vertx.ext.couchbase.impl.VertxJsonDocument} which the document id and JSON content.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @return a {@link io.vertx.ext.couchbase.impl.VertxJsonDocument}.
     */
    public static VertxJsonDocument create(String id, JsonObject content) {
        return new VertxJsonDocument(id, 0, content, 0);
    }

    /**
     * Creates a {@link io.vertx.ext.couchbase.impl.VertxJsonDocument} which the document id, JSON content and the CAS value.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @return a {@link io.vertx.ext.couchbase.impl.VertxJsonDocument}.
     */
    public static VertxJsonDocument create(String id, JsonObject content, long cas) {
        return new VertxJsonDocument(id, 0, content, cas);
    }

    /**
     * Creates a {@link io.vertx.ext.couchbase.impl.VertxJsonDocument} which the document id, JSON content and the expiration time.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param expiry the expiration time of the document.
     * @return a {@link io.vertx.ext.couchbase.impl.VertxJsonDocument}.
     */
    public static VertxJsonDocument create(String id, int expiry, JsonObject content) {
        return new VertxJsonDocument(id, expiry, content, 0);
    }

    /**
     * Creates a {@link io.vertx.ext.couchbase.impl.VertxJsonDocument} which the document id, JSON content, CAS value, expiration time and status code.
     *
     * This factory method is normally only called within the client library when a response is analyzed and a document
     * is returned which is enriched with the status code. It does not make sense to pre populate the status field from
     * the user level code.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @param expiry the expiration time of the document.
     * @return a {@link io.vertx.ext.couchbase.impl.VertxJsonDocument}.
     */
    public static VertxJsonDocument create(String id, int expiry, JsonObject content, long cas) {
        return new VertxJsonDocument(id, expiry, content, cas);
    }

    /**
     * Creates a copy from a different {@link io.vertx.ext.couchbase.impl.VertxJsonDocument}, but changes the document ID.
     *
     * @param doc the original {@link io.vertx.ext.couchbase.impl.VertxJsonDocument} to copy.
     * @param id the per-bucket unique document id.
     * @return a copied {@link io.vertx.ext.couchbase.impl.VertxJsonDocument} with the changed properties.
     */
    public static VertxJsonDocument from(VertxJsonDocument doc, String id) {
        return VertxJsonDocument.create(id, doc.expiry(), doc.content(), doc.cas());
    }

    /**
     * Creates a copy from a different {@link io.vertx.ext.couchbase.impl.VertxJsonDocument}, but changes the content.
     *
     * @param doc the original {@link io.vertx.ext.couchbase.impl.VertxJsonDocument} to copy.
     * @param content the content of the document.
     * @return a copied {@link io.vertx.ext.couchbase.impl.VertxJsonDocument} with the changed properties.
     */
    public static VertxJsonDocument from(VertxJsonDocument doc, JsonObject content) {
        return VertxJsonDocument.create(doc.id(), doc.expiry(), content, doc.cas());
    }

    /**
     * Creates a copy from a different {@link io.vertx.ext.couchbase.impl.VertxJsonDocument}, but changes the document ID and content.
     *
     * @param doc the original {@link io.vertx.ext.couchbase.impl.VertxJsonDocument} to copy.
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @return a copied {@link io.vertx.ext.couchbase.impl.VertxJsonDocument} with the changed properties.
     */
    public static VertxJsonDocument from(VertxJsonDocument doc, String id, JsonObject content) {
        return VertxJsonDocument.create(id, doc.expiry(), content, doc.cas());
    }

    /**
     * Creates a copy from a different {@link io.vertx.ext.couchbase.impl.VertxJsonDocument}, but changes the CAS value.
     *
     * @param doc the original {@link io.vertx.ext.couchbase.impl.VertxJsonDocument} to copy.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @return a copied {@link io.vertx.ext.couchbase.impl.VertxJsonDocument} with the changed properties.
     */
    public static VertxJsonDocument from(VertxJsonDocument doc, long cas) {
        return VertxJsonDocument.create(doc.id(), doc.expiry(), doc.content(), cas);
    }


    /**
     * Private constructor which is called by the static factory methods eventually.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @param expiry the expiration time of the document.
     */
    private VertxJsonDocument(String id, int expiry, JsonObject content, long cas) {
        super(id, expiry, content, cas);
    }

}


