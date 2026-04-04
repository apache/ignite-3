/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.worker;

import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Extension to standard {@link Runnable} interface.
 *
 * <p>Adds proper details to be used with {@link Executor} implementations.
 */
public abstract class IgniteWorker implements Runnable, WorkProgressDispatcher {
    /** Ignite logger. */
    protected final IgniteLogger log;

    /** Thread name. */
    private final String name;

    /** Ignite instance name. */
    private final String igniteInstanceName;

    /** Finish mark. */
    private volatile boolean finished;

    /** Whether this runnable is cancelled. */
    protected final AtomicBoolean isCancelled = new AtomicBoolean();

    /** Actual thread runner. */
    private volatile Thread runner;

    /** Timestamp to be updated by this worker periodically to indicate it's up and running. */
    private volatile long heartbeatTimestamp;

    /** Atomic field updater to change heartbeat. */
    private static final AtomicLongFieldUpdater<IgniteWorker> HEARTBEAT_UPDATER =
            AtomicLongFieldUpdater.newUpdater(IgniteWorker.class, "heartbeatTimestamp");

    /** Mutex for finish awaiting. */
    private final Object mux = new Object();

    /**
     * Creates new ignite worker with given parameters.
     *
     * @param log Logger.
     * @param igniteInstanceName Name of the Ignite instance this runnable is used in.
     * @param name Worker name. Note that in general thread name and worker (runnable) name are two different things.
     *      The same worker can be executed by multiple threads and therefore for logging and debugging purposes we separate the two.
     */
    protected IgniteWorker(
            IgniteLogger log,
            String igniteInstanceName,
            String name
    ) {
        this.log = log;
        this.igniteInstanceName = igniteInstanceName;
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override
    public final void run() {
        updateHeartbeat();

        // Runner thread must be recorded first as other operations may depend on it being present.
        runner = Thread.currentThread();

        log.debug("Ignite runnable started [name={}]", name);

        try {
            if (isCancelled.get()) {
                onCancelledBeforeWorkerScheduled();
            }

            body();
        } catch (InterruptedException e) {
            log.debug("Caught interrupted exception", e);

            Thread.currentThread().interrupt();
        } catch (Throwable e) {
            // Catch everything to make sure that it gets logged properly and
            // not to kill any threads from the underlying thread pool.

            log.warn("Runtime error caught during ignite runnable execution [worker={}]", e, this);

            if (e instanceof Error) {
                throw e;
            }
        } finally {
            synchronized (mux) {
                finished = true;

                mux.notifyAll();
            }

            cleanup();

            if (log.isDebugEnabled()) {
                if (isCancelled.get()) {
                    log.debug("Ignite runnable finished due to cancellation [threadName={}]", name);
                } else if (runner.isInterrupted()) {
                    log.debug("Ignite runnable finished due to interruption without cancellation [threadName={}]", name);
                } else {
                    log.debug("Ignite runnable finished normally [threadName={}]", name);
                }
            }

            // Need to set runner to null, to make sure that
            // further operations on this runnable won't
            // affect the thread which could have been recycled
            // by thread pool.
            runner = null;
        }
    }

    /**
     * The implementation should provide the execution body for this runnable.
     *
     * @throws InterruptedException Thrown in case of interruption.
     */
    protected abstract void body() throws InterruptedException;

    /**
     * Optional method that will be called after runnable is finished. Default implementation is no-op.
     */
    protected void cleanup() {
        /* No-op. */
    }

    /**
     * Returns runner thread, {@code null} if the worker has not yet started executing.
     */
    public @Nullable Thread runner() {
        return runner;
    }

    /**
     * Returns Name of the Ignite instance this runnable belongs to.
     */
    public String igniteInstanceName() {
        return igniteInstanceName;
    }

    /**
     * Returns this runnable name.
     */
    public String name() {
        return name;
    }

    /**
     * Cancels this runnable.
     */
    public void cancel() {
        log.debug("Cancelling ignite runnable [worker={}]", this);

        onCancel(isCancelled.compareAndSet(false, true));
    }

    /**
     * Joins this runnable.
     *
     * @throws InterruptedException Thrown in case of interruption.
     */
    public void join() throws InterruptedException {
        log.debug("Joining ignite runnable [worker={}]", this);

        if ((runner == null && isCancelled.get()) || finished) {
            return;
        }

        synchronized (mux) {
            while (!finished) {
                mux.wait();
            }
        }
    }

    /**
     * Returns {@code true} if this runnable is cancelled - {@code false} otherwise.
     *
     * @see Future#isCancelled()
     */
    public boolean isCancelled() {
        Thread runner = this.runner;

        return isCancelled.get() || (runner != null && runner.isInterrupted());
    }

    /**
     * Returns {@code true} if this runnable is finished - {@code false} otherwise.
     */
    public boolean isDone() {
        return finished;
    }

    /** {@inheritDoc} */
    @Override
    public long heartbeat() {
        return heartbeatTimestamp;
    }

    /** {@inheritDoc} */
    @Override
    public void updateHeartbeat() {
        long currentTimestamp = coarseCurrentTimeMillis();
        long heartbeatTimestamp = this.heartbeatTimestamp;

        // Avoid heartbeat update while in the blocking section.
        while (heartbeatTimestamp < currentTimestamp) {
            if (HEARTBEAT_UPDATER.compareAndSet(this, heartbeatTimestamp, currentTimestamp)) {
                return;
            }

            heartbeatTimestamp = this.heartbeatTimestamp;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void blockingSectionBegin() {
        heartbeatTimestamp = Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override
    public void blockingSectionEnd() {
        heartbeatTimestamp = coarseCurrentTimeMillis();
    }

    /**
     * Callback on runner cancellation.
     *
     * @param firstCancelRequest Flag indicating that worker cancellation was requested for the first time.
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected void onCancel(boolean firstCancelRequest) {
        Thread runner = this.runner;

        // Cannot apply Future.cancel() because if we do, then Future.get() would always
        // throw CancellationException, and we would not be able to wait for task completion.
        if (runner != null) {
            runner.interrupt();
        }
    }

    /**
     * Callback on special case, when task is cancelled before is has been scheduled.
     */
    protected void onCancelledBeforeWorkerScheduled() {
        Thread runner = this.runner;

        assert runner != null : this;

        runner.interrupt();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        Thread runner = this.runner;

        return S.toString(IgniteWorker.class, this,
                "hashCode", hashCode(),
                "interrupted", (runner != null ? runner.isInterrupted() : "unknown"),
                "runner", (runner == null ? "null" : runner.getName()));
    }
}
