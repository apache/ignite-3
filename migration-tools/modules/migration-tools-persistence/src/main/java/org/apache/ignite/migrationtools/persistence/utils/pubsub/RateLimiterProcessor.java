/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.persistence.utils.pubsub;

import java.util.concurrent.TimeUnit;

/** Limits the upstream rate of the downstream processor/publisher. */
public class RateLimiterProcessor<S> extends BasicProcessor<S, S> {
    private final int limit;

    private long currentWindow;

    private int currentCounter;

    /**
     * Constructor.
     *
     * @param timePeriod Duration.
     * @param timeUnit Unit of the duration.
     * @param limit Maximum number of elements processed during the period.
     */
    public RateLimiterProcessor(long timePeriod, TimeUnit timeUnit, int limit) {
        // Since the window is 1024, let's make some small adjustment.
        this.limit = (int) (limit * timeUnit.toSeconds(timePeriod) * 1024L / 1000L);
        this.currentWindow = 0;
        this.currentCounter = 0;
    }

    @Override
    public void onNext(S item) {
        long window = System.currentTimeMillis() >> 10;
        if (window > this.currentWindow) {
            resetWindow(window);
        } else if (this.currentCounter >= this.limit) {
            // Wait for the next window
            long nextWindow = window + 1;
            long delay = (nextWindow << 10) - System.currentTimeMillis();
            if (delay > 0) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    onError(e);
                }
            }
            resetWindow(nextWindow);
        }

        this.currentCounter++;
        subscriber.onNext(item);
    }

    private void resetWindow(long window) {
        this.currentWindow = window;
        this.currentCounter = 0;
    }
}
