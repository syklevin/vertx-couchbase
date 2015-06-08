package io.vertx.ext.couchbase.impl;

import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.java.transcoder.AbstractTranscoder;
import io.vertx.core.json.JsonObject;

/**
* Created by levin on 8/27/2014.
*/
public class VertxJsonTranscoder extends AbstractTranscoder<VertxJsonDocument, JsonObject> {
    @Override
    protected VertxJsonDocument doDecode(String id, ByteBuf content, long cas, int expiry, int flags, ResponseStatus status) throws Exception {
//        byte[] bytes = new byte[content.readableBytes()];
//        content.readBytes(bytes);
        JsonObject converted = new JsonObject(content.toString(CharsetUtil.UTF_8));
        return newDocument(id, expiry, converted, cas);
    }

    @Override
    protected Tuple2<ByteBuf, Integer> doEncode(VertxJsonDocument document) throws Exception {
        String content = document.content().encode();
        int flags = 0;
        return Tuple.create(Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), flags);
    }

    @Override
    public VertxJsonDocument newDocument(String id, int expiry, JsonObject content, long cas) {
        return VertxJsonDocument.create(id, expiry, content, cas);
    }

    @Override
    public Class<VertxJsonDocument> documentType() {
        return VertxJsonDocument.class;
    }
}
