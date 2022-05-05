package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.SCHEDULED;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.Nullable;

/**
 * Empty.
 */
// TODO: IGNITE-16898 Наполнить, написать тесты
class CheckpointProgressImpl implements CheckpointProgress {
    /**
     * Checkpoint id.
     */
    private final UUID id = UUID.randomUUID();

    /** Scheduled time of checkpoint. */
    private volatile long nextCheckpointNanos;

    /** Current checkpoint state. */
    private volatile AtomicReference<CheckpointState> state = new AtomicReference<>(SCHEDULED);

    /** Wakeup reason. */
    private volatile String reason;

    /** Number of dirty pages in current checkpoint at the beginning of checkpoint. */
    private volatile int currCheckpointPagesCnt;

    /** Cause of fail, which has happened during the checkpoint or null if checkpoint was successful. */
    private volatile Throwable failCause;

    /** Counter for written checkpoint pages. Not {@link null} only if checkpoint is running. */
    @Nullable
    private volatile AtomicInteger writtenPagesCntr;

    /** Counter for fsynced checkpoint pages. Not {@link null} only if checkpoint is running. */
    @Nullable
    private volatile AtomicInteger syncedPagesCntr;

    /** Counter for evicted checkpoint pages. Not {@link null} only if checkpoint is running. */
    @Nullable
    private volatile AtomicInteger evictedPagesCntr;

    /**
     * Constructor.
     *
     * @param nextCheckpointTimeout Timeout until next checkpoint.
     */
    CheckpointProgressImpl(long nextCheckpointTimeout) {
        // Avoid overflow on nextCpNanos.
        nextCheckpointTimeout = Math.min(TimeUnit.DAYS.toMillis(365), nextCheckpointTimeout);

        nextCheckpointNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(nextCheckpointTimeout);
    }

    /**
     * Returns scheduled time of checkpoint.
     */
    public long nextCheckpointNanos() {
        return nextCheckpointNanos;
    }

    /**
     * Sets new scheduled time of checkpoint.
     *
     * @param nextCheckpointNanos New scheduled time of checkpoint.
     */
    public void nextCheckpointNanos(long nextCheckpointNanos) {
        this.nextCheckpointNanos = nextCheckpointNanos;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<?> futureFor(CheckpointState state) {
        return null;
    }

    /**
     * Changing checkpoint state if order of state is correct.
     *
     * @param newState New checkpoint state.
     */
    public void transitTo(CheckpointState newState) {
    }

    /** {@inheritDoc} */
    @Override
    public int currentCheckpointPagesCount() {
        return currCheckpointPagesCnt;
    }

    /**
     * Sets current checkpoint pages num to store.
     *
     * @param num Pages to store.
     */
    public void currentCheckpointPagesCount(int num) {
        currCheckpointPagesCnt = num;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String reason() {
        return reason;
    }

    /**
     * Sets description of the reason of the current checkpoint.
     *
     * @param reason New wakeup reason.
     */
    public void reason(String reason) {
        this.reason = reason;
    }

    /** {@inheritDoc} */
    @Override
    public UUID id() {
        return id;
    }

    /**
     * Clear checkpoint progress counters.
     */
    public void clearCounters() {
        currCheckpointPagesCnt = 0;

        writtenPagesCntr = null;
        syncedPagesCntr = null;
        evictedPagesCntr = null;
    }

    /**
     * Initialize all counters before checkpoint.
     *
     * @param pagesSize Number of dirty pages in current checkpoint at the beginning of checkpoint.
     */
    public void initCounters(int pagesSize) {
        currCheckpointPagesCnt = pagesSize;

        writtenPagesCntr = new AtomicInteger();
        syncedPagesCntr = new AtomicInteger();
        evictedPagesCntr = new AtomicInteger();
    }

    /**
     * Mark this checkpoint execution as failed.
     *
     * @param error Causal error of fail.
     */
    public void fail(Throwable error) {
        failCause = error;

        transitTo(FINISHED);
    }

    /**
     * Returns {@code true} if current state equal to given state.
     *
     * @param expectedState Expected state.
     */
    public boolean greaterOrEqualTo(CheckpointState expectedState) {
        return state.get().ordinal() >= expectedState.ordinal();
    }
}
