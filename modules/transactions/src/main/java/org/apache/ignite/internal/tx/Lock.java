package org.apache.ignite.internal.tx;

import java.util.UUID;

/** Lock. */
public class Lock {

    /** Lock key. */
    private final LockKey lockKey;

    /** Lock mode. */
    private final LockMode lockMode;

    /** Transaction identificator. */
    private final UUID txId;

    /**
     * The constructor.
     *
     * @param lockKey Lock key.
     * @param lockMode Lock mode.
     * @param txId Transaction id.
     */
    public Lock(LockKey lockKey, LockMode lockMode, UUID txId) {
        this.lockKey = lockKey;
        this.lockMode = lockMode;
        this.txId = txId;
    }

    /**
     * Returns lock key.
     *
     * @return Lock key.
     */
    public LockKey lockKey() {
        return lockKey;
    }

    /**
     * Returns lock mode.
     *
     * @return Lock mode.
     */
    public LockMode lockMode() {
        return lockMode;
    }

    /**
     * Returns transcation identificator.
     *
     * @return Tx id.
     */
    public UUID txId() {
        return txId;
    }
}
