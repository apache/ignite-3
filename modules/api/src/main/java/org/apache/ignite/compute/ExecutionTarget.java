package org.apache.ignite.compute;

import org.apache.ignite.table.Tuple;

public interface ExecutionTarget {
    static ExecutionTarget anyNode() {
        return null; // TODO
    }

    static ExecutionTarget allNodes() {
        return null; // TODO
    }

    static ExecutionTarget fromNodeName(String name) {
        return null; // TODO
    }

    static ExecutionTarget fromColocationKey(Tuple key) {
        return null; // TODO
    }
}
