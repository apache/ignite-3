package org.apache.ignite.internal.sql.engine.exec.comp;

import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;

public interface SubscriptionManagementStrategy<T> extends Flow.Subscription {
    void addSubscription(Flow.Subscription subscription);

    void addSubscriber(Flow.Subscriber<T> subscriber);

    void push(int subscriberId, T item);

    void onSubscriptionComplete(int subscriberId);

    // should be reworked and removed
    boolean onRequestCompleted(int subscriberId);

    // should be removed
    void subscribe(Subscriber<? super T> delegate);
}
