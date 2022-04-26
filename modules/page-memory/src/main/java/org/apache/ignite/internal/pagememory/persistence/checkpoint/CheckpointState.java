package org.apache.ignite.internal.pagememory.persistence.checkpoint;

/**
 * Possible checkpoint states. Ordinal is important. Every next state follows the previous one.
 */
// TODO: IGNITE-16898 Review states
public enum CheckpointState {
    /** Checkpoint is waiting to execution. **/
    SCHEDULED,

    /** Checkpoint was awakened and it is preparing to start. **/
    LOCK_TAKEN,

    /** Dirty pages snapshot has been taken. **/
    PAGE_SNAPSHOT_TAKEN,

    /** Checkpoint counted the pages and write lock was released. **/
    LOCK_RELEASED,

    // TODO: IGNITE-16898 It will most likely need to be removed
    /** Checkpoint marker was stored to disk. **/
    MARKER_STORED_TO_DISK,

    /** Checkpoint was finished. **/
    FINISHED
}
