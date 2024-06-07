package org.apache.ignite.compute;

import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

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

    static ExecutionTarget fromColocationKey(String tableName, Tuple key) {
        return null; // TODO
    }

    static <K> ExecutionTarget fromColocationKey(String tableName, K key, Mapper<K> keyMapper) {
        return null; // TODO
    }
}
