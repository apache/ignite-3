package org.apache.ignite.internal.sql.engine.util;

import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;

public interface SubscriptionManagementStrategy<T> extends Flow.Subscription {
    // todo combine in composite publisher constructor?
    void addSubscription(Flow.Subscription subscription);

    Subscriber<T> subscriberProxy(int subscriberId);

    void onReceive(int subscriberId, T item);

    void onSubscriptionComplete(int subscriberId);

    // todo implementation can be reworked and this method should be removed
    void onRequestCompleted(int subscriberId);
}
