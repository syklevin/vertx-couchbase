package io.vertx.ext.couchbase.impl;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.subscriptions.BooleanSubscription;

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;

/**
 * Created by levin on 11/4/2014.
 */
public class ContextScheduler extends Scheduler {

    private final Vertx vertx;

    public ContextScheduler(Vertx vertx) {
        this.vertx = vertx;
    }


    @Override
    public Worker createWorker() {
        return new ContextWorker();
    }

    private class ContextWorker extends Worker {

        /**
         * Maintain list of all active timers
         */
        protected ArrayDeque<Long> timers = new ArrayDeque<>();

        /**
         * Cancel all timers
         */
        protected Action0 cancelAll = () -> {
            while (!timers.isEmpty())
                vertx.cancelTimer(timers.poll());
        };

        /**
         * Subscription with auto-cancel
         */
        protected BooleanSubscription innerSubscription = BooleanSubscription.create(cancelAll);

        // Scheduler.Worker implementation

        @Override
        public Subscription schedule(final Action0 action) {
            vertx.context().runOnContext(event -> {
                if (innerSubscription.isUnsubscribed())
                    return;
                action.call();
            });
            return this.innerSubscription;
        }

        @Override
        public Subscription schedule(final Action0 action, long delayTime, TimeUnit unit) {
            timers.add(vertx.setTimer(unit.toMillis(delayTime), id -> {
                if (innerSubscription.isUnsubscribed())
                    return;
                action.call();
                timers.remove(id);
            }));
            return this.innerSubscription;
        }

        @Override
        public Subscription schedulePeriodically(final Action0 action, long initialDelay, final long delayTime, final TimeUnit unit) {

            // Use a bootstrap handler to start the periodic timer after initialDelay
            Handler<Long> bootstrap = ignored -> {

                action.call();

                // Ensure still active
                if (innerSubscription.isUnsubscribed())
                    return;

                // Start the repeating timer
                timers.add(vertx.setPeriodic(unit.toMillis(delayTime), nestedId -> {
                    if (innerSubscription.isUnsubscribed())
                        return;
                    action.call();
                }));
            };

            long bootDelay = unit.toMillis(initialDelay);

            // If initialDelay is 0 then fire bootstrap immediately
            if (bootDelay < 1) {
                vertx.runOnContext(v -> bootstrap.handle(null));
            } else {
                timers.add(vertx.setTimer(bootDelay, bootstrap));
            }

            return this.innerSubscription;
        }

        @Override
        public void unsubscribe() {
            innerSubscription.unsubscribe();
        }

        @Override
        public boolean isUnsubscribed() {
            return innerSubscription.isUnsubscribed();
        }
    }
}
