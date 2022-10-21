package org.apache.ignite.internal.sql.engine.util;

import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;

public interface SubscriptionManagementStrategy<T> extends Flow.Subscription {
    // todo combine in composite publisher constructor?
    void addSubscription(Flow.Subscription subscription);
    void addSubscriber(Flow.Subscriber<T> subscriber);

    void onReceive(int subscriberId, T item);

    void onSubscriptionComplete(int subscriberId);

    // todo should be reworked and removed
    boolean onRequestCompleted(int subscriberId);

    // todo should be removed
    void subscribe(Subscriber<? super T> delegate);
}
