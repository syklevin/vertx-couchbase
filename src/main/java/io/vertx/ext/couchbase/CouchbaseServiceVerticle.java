package io.vertx.ext.couchbase;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.serviceproxy.ProxyHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by levin on 1/19/2015.
 */
public class CouchbaseServiceVerticle extends AbstractVerticle {
    public static final Logger logger = LogManager.getLogger(CouchbaseServiceVerticle.class);
    CouchbaseService service;

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        // And register it on the event bus against the configured address
        String address = config().getString("address");
        if (address == null) {
            startFuture.fail(new IllegalStateException("address field must be specified in config for service verticle"));
            return;
        }

        service = CouchbaseService.create(vertx, config());

        ProxyHelper.registerService(CouchbaseService.class, vertx, service, address);

        // Start it
        service.start(ar -> {
            if (ar.succeeded()) {
                startFuture.complete();
            } else {
                startFuture.fail(ar.cause());
            }
        });
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        service.stop(ar -> {
            if (ar.succeeded()) {
                stopFuture.complete();
            } else {
                stopFuture.fail(ar.cause());
            }
        });
    }

}
