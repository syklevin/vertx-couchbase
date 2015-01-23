package io.vertx.ext.couchbase;

import io.vertx.ext.couchbase.impl.VertxScheduler;
import io.vertx.test.core.VertxTestBase;
import org.junit.Assert;
import org.junit.Test;
import rx.Observable;

import java.util.concurrent.CountDownLatch;

/**
 * Created by levin on 1/22/2015.
 */
public class AsyncTest extends VertxTestBase {

    @Test
    public void testAsync() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("start wait for 1 seconds");
                    Thread.sleep(1000);
                    System.out.println("finish wait for 1 seconds");
                    Assert.assertTrue(true);
                    latch.countDown();
                }
                catch (Exception ex){}
            }
        }).start();

        latch.await();
    }

    @Test
    public void testVertxAsync() throws Exception {
        vertx.runOnContext(ctx -> {
            try {
                System.out.println("start wait for 500 millis");
                Thread.sleep(500);
                System.out.println("finish wait for 500 millis");
                Assert.assertTrue(true);
                testComplete();
            } catch (Exception ex) {
            }
        });

        await();
    }

    @Test
    public void testRxAsync() throws Exception {

        Observable
                .create(obs -> {
                    try {
                        System.out.println("start wait for 1 seconds");
                        Thread.sleep(1000);
                        System.out.println("finish wait for 1 seconds");
                        obs.onNext(null);
                    } catch (Exception ex) {
                    } finally {
                        obs.onCompleted();
                    }
                })
                .subscribeOn(new VertxScheduler(vertx))
                .subscribe(ar -> {
                    Assert.assertTrue(true);
                    testComplete();
                }, e -> {
                    fail(e.getMessage());
                });
        await();
    }
}
