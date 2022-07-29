package org.apache.ignite.internal.tx;

public enum LockMode {
    SHARED,
    EXCLUSIVE,
    INTENTION_SHARED,
    INTENTION_EXCLUSIVE,
    SHARED_AND_INTENTION_EXCLUSIVE
}
