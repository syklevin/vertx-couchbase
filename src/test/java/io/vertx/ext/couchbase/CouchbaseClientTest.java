package io.vertx.ext.couchbase;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.CouchbaseAsyncCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.ViewQuery;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;
import rx.Observable;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by tommykwan on 5/2/15.
 */
public class CouchbaseClientTest extends VertxTestBase {

    AsyncBucket asyncBucket;

    @Override
    public void setUp() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        String bucket = "helios-test";
        DefaultCouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().build();
        CouchbaseAsyncCluster couchbaseAsyncCluster = CouchbaseAsyncCluster.create(env, new String[] { "192.168.0.87" });
        couchbaseAsyncCluster.openBucket(bucket, "")
            .subscribe(result -> {
                asyncBucket = result;
                latch.countDown();
            });
        latch.await();
    }

    @Test
    public void testReplace() throws InterruptedException {
        int count = 100;
        CountDownLatch latch = new CountDownLatch(count);
        long startTime = System.currentTimeMillis();
        String doctype = "test";
        String id = doctype + "::" + UUID.randomUUID().toString();
        ArrayList<String> arr = new ArrayList<>();
        for (int i = 0; i < count; ++i) arr.add(id);
        final int[] successCount = {0};
        final int[] delay = {0};
        Observable.from(arr)
            .flatMap(x -> Observable.just(x).delay(delay[0]++ * 100, TimeUnit.MILLISECONDS))
            .flatMap(x -> update(x))
            .subscribe(x -> {
                    System.out.println("done: " + (System.currentTimeMillis() - startTime) + " => " + successCount[0]++);
                    latch.countDown();
                }, error -> System.out.println(error)
            );
        latch.await();
    }

    public Observable<JsonDocument> update(String id) {
        return asyncBucket
            .upsert(JsonDocument.create(id, JsonObject.create().put("hello", new Date().toString())))
            .defer(() -> asyncBucket.get(id))
            .flatMap(doc -> asyncBucket.replace(doc))
            .retryWhen(o -> {
                return o.flatMap(n -> {
                    return Observable.timer(10, TimeUnit.MILLISECONDS);
                });
            });
    }

    @Test
    public void testDelay() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(5000);
        ArrayList<Integer> arr = new ArrayList<>();
        for (int i = 0; i < 5000; ++i) arr.add(i);
        long startTime = System.currentTimeMillis();
        final int[] delay = {0};
        Observable.from(arr)
            .flatMap(x -> Observable.just(x).delay(delay[0]++, TimeUnit.MILLISECONDS))
            .subscribe(x -> {
                System.out.println(x + " " + (System.currentTimeMillis() - startTime) + "ms");
                latch.countDown();
            });
        latch.await();
    }

    @Test
    public void testFindOneFail() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        asyncBucket
            .get("injofejwfioj3")
            .first()
            .subscribe(x -> {
                latch.countDown();
            }, System.err::println);
        latch.await();
    }

    @Test
    public void testViewQueryCount() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ViewQuery viewQuery = ViewQuery.from("dev_test", "test_group");
        viewQuery.reduce(true);
        viewQuery.group(true);
        asyncBucket
            .query(viewQuery)
            .flatMap(AsyncViewResult::rows)
            .flatMap(x -> {
                Map<Object, Object> keyValue = new HashMap<>();
                keyValue.put("key", x.key());
                keyValue.put("value", x.value());
                return Observable.just(keyValue);
            })
            .toList()
            .subscribe(x -> {
                latch.countDown();
            }, System.err::println);
        latch.await();
    }

}
