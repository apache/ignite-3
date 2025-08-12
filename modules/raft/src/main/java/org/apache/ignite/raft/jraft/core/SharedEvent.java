package org.apache.ignite.raft.jraft.core;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.FSMCallerImpl.IApplyTask;
import org.apache.ignite.raft.jraft.core.FSMCallerImpl.TaskType;
import org.apache.ignite.raft.jraft.core.NodeImpl.ILogEntryAndClosure;
import org.apache.ignite.raft.jraft.disruptor.NodeIdAware;
import org.apache.ignite.raft.jraft.entity.LeaderChangeContext;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.storage.impl.LogManagerImpl.EventType;
import org.apache.ignite.raft.jraft.storage.impl.LogManagerImpl.IStableClosureEvent;

public class SharedEvent extends NodeIdAware implements IApplyTask, ILogEntryAndClosure, IStableClosureEvent {
    public TaskType type;
    // union fields
    public long committedIndex;
    long term;
    Status status;
    LeaderChangeContext leaderChangeCtx;
    Closure done;

    LogEntry entry;
    long expectedTerm;
    CountDownLatch shutdownLatch;

    EventType eventType;

    @Override
    public TaskType getType() {
        return type;
    }

    @Override
    public void setType(TaskType type) {
        this.type = type;
    }

    @Override
    public long getCommittedIndex() {
        return committedIndex;
    }

    @Override
    public void setCommittedIndex(long committedIndex) {
        this.committedIndex = committedIndex;
    }

    @Override
    public long getTerm() {
        return term;
    }

    @Override
    public void setTerm(long term) {
        this.term = term;
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public void setStatus(Status status) {
        this.status = status;
    }

    @Override
    public LeaderChangeContext getLeaderChangeCtx() {
        return leaderChangeCtx;
    }

    @Override
    public void setLeaderChangeCtx(LeaderChangeContext leaderChangeCtx) {
        this.leaderChangeCtx = leaderChangeCtx;
    }

    @Override
    public Closure getDone() {
        return done;
    }

    @Override
    public void setDone(Closure done) {
        this.done = done;
    }

    @Override
    public CountDownLatch getShutdownLatch() {
        return shutdownLatch;
    }

    @Override
    public void setShutdownLatch(CountDownLatch shutdownLatch) {
        this.shutdownLatch = shutdownLatch;
    }

    @Override
    public LogEntry getEntry() {
        return entry;
    }

    @Override
    public void setEntry(LogEntry entry) {
        this.entry = entry;
    }

    @Override
    public long getExpectedTerm() {
        return expectedTerm;
    }

    @Override
    public void setExpectedTerm(long expectedTerm) {
        this.expectedTerm = expectedTerm;
    }

    @Override
    public EventType getEventType() {
        return eventType;
    }

    @Override
    public void setEventType(EventType type) {
        this.eventType = type;
    }

    @Override
    public void reset() {
        super.reset();

        this.entry = null;
        this.done = null;
        this.expectedTerm = 0;
        this.shutdownLatch = null;

        this.type = null;
        this.committedIndex = 0;
        this.term = 0;
        this.status = null;
        this.leaderChangeCtx = null;
        this.done = null;

        this.done = null;
        this.type = null;
    }

    @Override
    public String toString() {
        return "SharedEvent{" +
                "type=" + type +
                ", committedIndex=" + committedIndex +
                ", term=" + term +
                ", status=" + status +
                ", leaderChangeCtx=" + leaderChangeCtx +
                ", done=" + done +
                ", entry=" + entry +
                ", expectedTerm=" + expectedTerm +
                ", shutdownLatch=" + shutdownLatch +
                ", eventType=" + eventType +
                ", parent.nodeId=" + nodeId +
                ", parent.handler=" + handler +
                ", parent.disEvtType=" + evtType +
                ", parent.srcType=" + srcType +
                '}';
    }
}
