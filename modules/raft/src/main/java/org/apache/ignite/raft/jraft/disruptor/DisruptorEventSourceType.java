package org.apache.ignite.raft.jraft.disruptor;

public enum DisruptorEventSourceType {
    APPLY_TASK,
    LOG,
    RO,
    FSM
}
