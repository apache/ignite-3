package org.apache.ignite.internal.raft;

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Implementation of {@link ThrottlingContextHolder} for testing purposes.
 */
public class TestThrottlingContextHolder implements ThrottlingContextHolder {
    private final long peerRequestTimeoutMillis;

    /**
     * Constructor.
     *
     * @param peerRequestTimeoutMillis Peer request timeout in milliseconds.
     */
    public TestThrottlingContextHolder(long peerRequestTimeoutMillis) {
        this.peerRequestTimeoutMillis = peerRequestTimeoutMillis;
    }

    /**
     * Creates a new instance of {@link ThrottlingContextHolder} for testing purposes with a default timeout of 3000 ms.
     *
     * @return A new instance of {@link ThrottlingContextHolder} configured for testing.
     */
    @TestOnly
    public static ThrottlingContextHolder throttlingContextHolder() {
        return testThrottlingContext(3000L);
    }

    /**
     * Creates a new instance of {@link ThrottlingContextHolder} for testing purposes with a specified timeout.
     *
     * @param peerRequestTimeoutMillis Peer request timeout in milliseconds.
     * @return A new instance of {@link ThrottlingContextHolder} configured for testing with the specified timeout.
     */
    @TestOnly
    public static ThrottlingContextHolder testThrottlingContext(long peerRequestTimeoutMillis) {
        return new TestThrottlingContextHolder(peerRequestTimeoutMillis);
    }

    @Override
    public boolean isOverloaded(Peer peer) {
        return false;
    }

    @Override
    public void beforeRequest(Peer peer) {
        // No-op.
    }

    @Override
    public void afterRequest(Peer peer, long requestStartTimestamp, @Nullable Throwable err) {
        // No-op.
    }

    @Override
    public long peerRequestTimeoutMillis(Peer peer) {
        return peerRequestTimeoutMillis;
    }
}
