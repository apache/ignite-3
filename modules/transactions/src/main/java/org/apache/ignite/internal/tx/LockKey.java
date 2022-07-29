package org.apache.ignite.internal.tx;

import java.util.UUID;

public class LockKey {

    private final UUID contextId;

    private final Object key;

    public LockKey(Object key) {
        this(null, key);
    }

    public LockKey(UUID contextId, Object key) {
        this.contextId = contextId;
        this.key = key;
    }

    /**
     * @return Context id.
     */
    public UUID contextId() {
        return contextId;
    }

    /**
     * @return Key.
     */
    public Object key() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LockKey lockKey = (LockKey) o;

        if (contextId != null ? !contextId.equals(lockKey.contextId) : lockKey.contextId != null) {
            return false;
        }
        return key != null ? key.equals(lockKey.key) : lockKey.key == null;
    }

    @Override
    public int hashCode() {
        int result = contextId != null ? contextId.hashCode() : 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        return result;
    }
}
