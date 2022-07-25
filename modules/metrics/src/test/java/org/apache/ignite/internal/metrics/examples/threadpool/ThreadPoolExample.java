package org.apache.ignite.internal.metrics.examples.threadpool;

import org.apache.ignite.internal.metrics.MetricsRegistry;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolExample {
    public static void main(String[] args) {
        // Should be one per node.
        MetricsRegistry registry = new MetricsRegistry();

        // ------------------------------------------------------------------------

        // System component, e.g. thread pool executor
        ThreadPoolExecutor exec = new ThreadPoolExecutor(4, 4,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

        // Metrics source for thread pool
        ThreadPoolMetricsSource src = new ThreadPoolMetricsSource("example.thread_pool.ExamplePool", exec);

        // Register source after the component created.
        registry.registerSource(src);

        // ------------------------------------------------------------------------

        // Enable metrics by signal (or because configuration)
        registry.enable(src.name());

        // ------------------------------------------------------------------------

        // Disable metrics by signal
        registry.disable(src.name());

        // ------------------------------------------------------------------------

        // Component is stopped\destroyed
        registry.unregisterSource(src);
    }
}
