package org.apache.ignite.internal.compute.queue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper for {@link ThreadPoolExecutor}. Allows removing task from work queue.
 */
public class ComputeThreadPoolExecutor {

    private final BlockingQueue<Runnable> workQueue;

    private final ThreadPoolExecutor executor;

    ComputeThreadPoolExecutor(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            ThreadFactory threadFactory) {
        this.workQueue = workQueue;
        executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    /**
     * {@link ThreadPoolExecutor#execute(Runnable command)}
     */
    public void execute(Runnable command) {
        executor.execute(command);
    }

    /**
     * {@link ThreadPoolExecutor#remove(Runnable command)}
     */
    public boolean remove(Runnable task) {
        return executor.remove(task);
    }

    /**
     * Removes this task from the executor's internal queue if it is
     * present, thus causing it not to be run if it has not already
     * started.
     *
     * @param task the task to remove
     * @return {@code true} if the task was removed
     */
    public boolean removeFromQueue(Runnable task) {
        return workQueue.remove(task);
    }

    /**
     * Getter for internal execution service
     *
     * @return internal execution service
     */
    ExecutorService executorService() {
        return executor;
    }


}
