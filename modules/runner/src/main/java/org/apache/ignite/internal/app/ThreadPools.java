package org.apache.ignite.internal.app;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.thread.LogUncaughtExceptionHandler;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Component that hosts thread pools which do not belong to a certain component and which are global to an Ignite instance.
 */
public class ThreadPools implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(ThreadPools.class);

    private final String nodeName;

    private final StripedThreadPoolExecutor partitionOperationsExecutor;

    public ThreadPools(String nodeName) {
        this.nodeName = nodeName;

        partitionOperationsExecutor = new StripedThreadPoolExecutor(
                Math.min(Runtime.getRuntime().availableProcessors() * 3, 25),
                NamedThreadFactory.threadPrefix(this.nodeName, "partition-operations"),
                new LogUncaughtExceptionHandler(LOG),
                false,
                0
        );
    }

    @Override
    public void start() {
        // No-op.
    }

    @Override
    public void stop() throws Exception {
        IgniteUtils.shutdownAndAwaitTermination(partitionOperationsExecutor, 10, TimeUnit.SECONDS);
    }

    public StripedThreadPoolExecutor partitionOperationsExecutor() {
        return partitionOperationsExecutor;
    }
}
