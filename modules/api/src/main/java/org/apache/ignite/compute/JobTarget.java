package org.apache.ignite.compute;

import java.util.Collection;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

/**
 * Job execution target.
 */
public interface JobTarget {
    static JobTarget node(ClusterNode node) {
        return null; // TODO
    }

    static JobTarget anyNode(ClusterNode... nodes) {
        return null; // TODO
    }

    static JobTarget anyNode(Collection<ClusterNode> nodes) {
        return null; // TODO
    }

    static JobTarget colocated(String tableName, Tuple key) {
        return null; // TODO
    }

    static <K> JobTarget colocated(String tableName, K key, Mapper<K> keyMapper) {
        return null; // TODO
    }
}
