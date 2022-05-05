package org.apache.ignite.internal.util.worker;


/**
 * Dispatcher of workers' progress which allows us to understand if worker freezes.
 */
public interface WorkProgressDispatcher {
    /**
     * Last heartbeat timestamp.
     */
    long heartbeat();

    /**
     * Notifying dispatcher that work is in progress and thread didn't freeze.
     */
    void updateHeartbeat();

    /**
     * Protects the worker from timeout penalties if subsequent instructions in the calling thread does not update heartbeat timestamp
     * timely, e.g. due to blocking operations, up to the nearest {@link #blockingSectionEnd()} call. Nested calls are not supported.
     */
    void blockingSectionBegin();

    /**
     * Closes the protection section previously opened by {@link #blockingSectionBegin()}.
     */
    void blockingSectionEnd();
}
