package org.apache.ignite.network;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Lazy collection of single-thread executors indexed with short type. An executor is created on first access and remains active forever.
 *
 * <p>After having been stopped, it always returns an 'executor' that never executes anything:
 * if returns failed futures, throws an ExecutionException if forced to return some execution result, or just does
 * nothing when asked to execute a task if it is not required to return either future or execution result.
 */
class LazyExecutorCollection implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(LazyExecutorCollection.class);

    private static final ExecutorService NO_OP_EXECUTOR = new ReluctantExecutorService();

    private final String nodeName;
    private final String poolName;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();
    private final AtomicReferenceArray<ExecutorService> array = new AtomicReferenceArray<>(Short.MAX_VALUE + 1);

    LazyExecutorCollection(String nodeName, String poolName) {
        this.nodeName = nodeName;
        this.poolName = poolName;
    }

    /**
     * Returns an executor by the given index. If the collection is stopped, returns a non-executor that executes nothing.
     *
     * @param index Index of the executor to return.
     */
    public ExecutorService executor(short index) {
        if (!busyLock.enterBusy()) {
            return NO_OP_EXECUTOR;
        }

        try {
            return array.updateAndGet(
                    index,
                    existing -> existing != null
                            ? existing
                            : Executors.newSingleThreadExecutor(NamedThreadFactory.create(nodeName, poolName + "-" + index, LOG))
            );
        } finally {
            busyLock.leaveBusy();
        }
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

    /**
     * Never executes anything. If it's forced to return a future, it returns a failed future; if forced to return execution result, it
     * throws ExecutionException; otherwise it just does nothing.
     *
     * <p>Shutdown attempts lead to {@link UnsupportedOperationException}s being thrown.
     */
    private static class ReluctantExecutorService implements ExecutorService {
        @Override
        public void shutdown() {
            throw new UnsupportedOperationException("This should never be shut down");
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException("This should never be shut down");
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException("This should never be shut down");
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return failedFuture(new NodeStoppingException());
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return failedFuture(new NodeStoppingException());
        }

        @Override
        public Future<?> submit(Runnable task) {
            return failedFuture(new NodeStoppingException());
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
            return tasks.stream().map(this::submit).collect(toList());
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
            return tasks.stream().map(this::submit).collect(toList());
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws ExecutionException {
            throw new ExecutionException(new NodeStoppingException());
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws ExecutionException {
            throw new ExecutionException(new NodeStoppingException());
        }

        @Override
        public void execute(Runnable command) {
            // No-op.
        }
    }
}
