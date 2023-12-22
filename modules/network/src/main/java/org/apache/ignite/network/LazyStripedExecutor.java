package org.apache.ignite.network;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Lazy striped executor indexed with short type. A thread is created on first execution with an index and remains active forever.
 *
 * <p>After having been stopped, it never executes anything.
 */
class LazyStripedExecutor implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(LazyStripedExecutor.class);

    private final String nodeName;
    private final String poolName;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();
    private final AtomicReferenceArray<ExecutorService> array = new AtomicReferenceArray<>(Short.MAX_VALUE + 1);

    LazyStripedExecutor(String nodeName, String poolName) {
        this.nodeName = nodeName;
        this.poolName = poolName;
    }

    /**
     * Executes a command on a stripe with the given index. If the executor is stopped, does nothing.
     *
     * @param index Index of the stripe.
     */
    public void execute(short index, Runnable command) {
        assert index >= 0 : "Index is negative: " + index;

        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            executorFor(index).execute(command);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private Executor executorFor(short index) {
        return array.updateAndGet(
                index,
                existing -> existing != null
                        ? existing
                        : Executors.newSingleThreadExecutor(NamedThreadFactory.create(nodeName, poolName + "-" + index, LOG))
        );
    }

    @Override
    public void close() {
        busyLock.block();

        IntStream.range(0, array.length())
                .mapToObj(array::get)
                .filter(Objects::nonNull)
                .parallel()
                .forEach(executorService -> IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS));
    }
}
