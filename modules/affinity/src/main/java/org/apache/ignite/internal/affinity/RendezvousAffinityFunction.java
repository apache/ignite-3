/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.affinity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.NetworkMember;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Affinity function for partitioned cache based on Highest Random Weight algorithm.
 * This function supports the following configuration:
 * <ul>
 * <li>
 *      {@code partitions} - Number of partitions to spread across nodes.
 * </li>
 * <li>
 *      {@code excludeNeighbors} - If set to {@code true}, will exclude same-host-neighbors
 *      from being backups of each other. This flag can be ignored in cases when topology has no enough nodes
 *      for assign backups.
 *      Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
 * </li>
 * <li>
 *      {@code backupFilter} - Optional filter for back up nodes. If provided, then only
 *      nodes that pass this filter will be selected as backup nodes. If not provided, then
 *      primary and backup nodes will be selected out of all nodes available for this cache.
 * </li>
 * </ul>
 */
public class RendezvousAffinityFunction implements AffinityFunction, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default number of partitions. */
    public static final int DFLT_PARTITION_COUNT = 1024;

    /** Comparator. */
    private static final Comparator<IgniteBiTuple<Long, NetworkMember>> COMPARATOR = new HashComparator();

    /** Maximum number of partitions. */
    public static final int MAX_PARTITIONS_COUNT = 65000;

    /** Number of partitions. */
    private int parts;

    /** Mask to use in calculation when partitions count is power of 2. */
    private int mask = -1;

    /** Exclude neighbors flag. */
    private boolean exclNeighbors;

    /** Exclude neighbors warning. */
    private transient boolean exclNeighborsWarn;

    /** Optional affinity backups filter. The first node is a node being tested,
     *  the second is a list of nodes that are already assigned for a given partition (the first node in the list
     *  is primary). */
    private IgniteBiPredicate<NetworkMember, List<NetworkMember>> affinityBackupFilter;

    /** Logger instance. */
    private transient Logger log = LoggerFactory.getLogger(RendezvousAffinityFunction.class);

    /**
     * Helper method to calculates mask.
     *
     * @param parts Number of partitions.
     * @return Mask to use in calculation when partitions count is power of 2.
     */
    public static int calculateMask(int parts) {
        return (parts & (parts - 1)) == 0 ? parts - 1 : -1;
    }

    /**
     * Helper method to calculate partition.
     *
     * @param key – Key to get partition for.
     * @param mask Mask to use in calculation when partitions count is power of 2.
     * @param parts Number of partitions.
     * @return Partition number for a given key.
     */
    public static int calculatePartition(Object key, int mask, int parts) {
        if (mask >= 0) {
            int h;

            return ((h = key.hashCode()) ^ (h >>> 16)) & mask;
        }

        return safeAbs(key.hashCode() % parts);
    }

    /**
     * Initializes affinity with flag to exclude same-host-neighbors from being backups of each other
     * and specified number of backups.
     * <p>
     * Note that {@code affinityBackupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups
     *      of each other.
     */
    public RendezvousAffinityFunction(boolean exclNeighbors) {
        this(exclNeighbors, DFLT_PARTITION_COUNT);
    }

    /**
     * Initializes affinity with flag to exclude same-host-neighbors from being backups of each other,
     * and specified number of backups and partitions.
     * <p>
     * Note that {@code affinityBackupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups
     *      of each other.
     * @param parts Total number of partitions.
     */
    public RendezvousAffinityFunction(
        boolean exclNeighbors,
        int parts) {
        this(exclNeighbors, parts, null);
    }

    /**
     * Initializes optional counts for replicas and backups.
     * <p>
     * Note that {@code affinityBackupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param parts Total number of partitions.
     * @param affinityBackupFilter Optional back up filter for nodes. If provided, backups will be selected
     *      from all nodes that pass this filter. First argument for this filter is primary node, and second
     *      argument is node being tested.
     * <p>
     * Note that {@code affinityBackupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     */
    public RendezvousAffinityFunction(
        int locallyGeneratedId,
        int parts,
        IgniteBiPredicate<NetworkMember, List<NetworkMember>> affinityBackupFilter
    ) {
        this(false, parts, affinityBackupFilter);
    }

    /**
     * Private constructor.
     *
     * @param exclNeighbors Exclude neighbors flag.
     * @param parts Partitions count.
     * @param affinityBackupFilter Backup filter.
     */
    private RendezvousAffinityFunction(
        boolean exclNeighbors,
        int parts,
        IgniteBiPredicate<NetworkMember, List<NetworkMember>> affinityBackupFilter
    ) {
        assert parts > 0 : "parts > 0";

        this.exclNeighbors = exclNeighbors;
        this.affinityBackupFilter = affinityBackupFilter;

        setPartitions(parts);
    }

    /**
     * Gets total number of key partitions. To ensure that all partitions are
     * equally distributed across all nodes, please make sure that this
     * number is significantly larger than a number of nodes. Also, partition
     * size should be relatively small. Try to avoid having partitions with more
     * than quarter million keys.
     * <p>
     * For fully replicated caches this method works the same way as a partitioned
     * cache.
     *
     * @return Total partition count.
     */
    public int getPartitions() {
        return parts;
    }

    /**
     * Sets total number of partitions.If the number of partitions is a power of two,
     * the PowerOfTwo hashing method will be used.  Otherwise the Standard hashing
     * method will be applied.
     *
     * @param parts Total number of partitions.
     * @return {@code this} for chaining.
     */
    public RendezvousAffinityFunction setPartitions(int parts) {
        assert parts <= MAX_PARTITIONS_COUNT : "parts <= " + MAX_PARTITIONS_COUNT;
        assert parts > 0: "parts > 0";

        this.parts = parts;

        mask = calculateMask(parts);

        return this;
    }

    /**
     * Gets optional backup filter. If not {@code null}, backups will be selected
     * from all nodes that pass this filter. First node passed to this filter is a node being tested,
     * and the second parameter is a list of nodes that are already assigned for a given partition (primary node is the first in the list).
     * <p>
     * Note that {@code affinityBackupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}. 
     *
     * @return Optional backup filter.
     */
    public IgniteBiPredicate<NetworkMember, List<NetworkMember>> getAffinityBackupFilter() {
        return affinityBackupFilter;
    }

    /**
     * Sets optional backup filter. If provided, then backups will be selected from all
     * nodes that pass this filter. First node being passed to this filter is a node being tested,
     * and the second parameter is a list of nodes that are already assigned for a given partition (primary node is the first in the list).
     * <p>
     * Note that {@code affinityBackupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.     
     * <p>
     *
     * @param affinityBackupFilter Optional backup filter.
     * @return {@code this} for chaining.
     */
    public RendezvousAffinityFunction setAffinityBackupFilter(
        IgniteBiPredicate<NetworkMember, List<NetworkMember>> affinityBackupFilter) {
        this.affinityBackupFilter = affinityBackupFilter;

        return this;
    }

    /**
     * Checks flag to exclude same-host-neighbors from being backups of each other (default is {@code false}).
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @return {@code True} if nodes residing on the same host may not act as backups of each other.
     */
    public boolean isExcludeNeighbors() {
        return exclNeighbors;
    }

    /**
     * Sets flag to exclude same-host-neighbors from being backups of each other (default is {@code false}).
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups of each other.
     * @return {@code this} for chaining.
     */
    public RendezvousAffinityFunction setExcludeNeighbors(boolean exclNeighbors) {
        this.exclNeighbors = exclNeighbors;

        return this;
    }

    /**
     * Resolves node hash.
     *
     * @param node Cluster node;
     * @return Node hash.
     */
    public Object resolveNodeHash(NetworkMember node) {
        return node.name();
    }

    /**
     * Returns collection of nodes (primary first) for specified partition.
     *
     * @param part Partition.
     * @param nodes Nodes.
     * @param backups Number of backups.
     * @param neighborhoodCache Neighborhood.
     * @return Assignment.
     */
    public List<NetworkMember> assignPartition(int part,
        List<NetworkMember> nodes,
        int backups,
        Map<UUID, Collection<NetworkMember>> neighborhoodCache) {
        if (nodes.size() <= 1)
            return nodes;

        IgniteBiTuple<Long, NetworkMember>[] hashArr =
            (IgniteBiTuple<Long, NetworkMember>[])new IgniteBiTuple[nodes.size()];

        for (int i = 0; i < nodes.size(); i++) {
            NetworkMember node = nodes.get(i);

            Object nodeHash = resolveNodeHash(node);

            long hash = hash(nodeHash.hashCode(), part);

            hashArr[i] = new IgniteBiTuple<>(hash, node);
        }

        final int primaryAndBackups = backups == Integer.MAX_VALUE ? nodes.size() : Math.min(backups + 1, nodes.size());

        Iterable<NetworkMember> sortedNodes = new LazyLinearSortedContainer(hashArr, primaryAndBackups);

        // REPLICATED cache case
        if (backups == Integer.MAX_VALUE)
            return replicatedAssign(nodes, sortedNodes);

        Iterator<NetworkMember> it = sortedNodes.iterator();

        List<NetworkMember> res = new ArrayList<>(primaryAndBackups);

        Collection<NetworkMember> allNeighbors = new HashSet<>();

        NetworkMember primary = it.next();

        res.add(primary);

        if (exclNeighbors)
            allNeighbors.addAll(neighborhoodCache.get(primary.id()));

        // Select backups.
        if (backups > 0) {
            while (it.hasNext() && res.size() < primaryAndBackups) {
                NetworkMember node = it.next();

                if (exclNeighbors) {
                    if (!allNeighbors.contains(node)) {
                        res.add(node);

                        allNeighbors.addAll(neighborhoodCache.get(node.id()));
                    }
                }
                else if ((affinityBackupFilter != null && affinityBackupFilter.apply(node, res))
                    || affinityBackupFilter == null) {
                    res.add(node);

                    if (exclNeighbors)
                        allNeighbors.addAll(neighborhoodCache.get(node.id()));
                }
            }
        }

        if (res.size() < primaryAndBackups && nodes.size() >= primaryAndBackups && exclNeighbors) {
            // Need to iterate again in case if there are no nodes which pass exclude neighbors backups criteria.
            it = sortedNodes.iterator();

            it.next();

            while (it.hasNext() && res.size() < primaryAndBackups) {
                NetworkMember node = it.next();

                if (!res.contains(node))
                    res.add(node);
            }

            if (!exclNeighborsWarn) {
                log.warn("Affinity function excludeNeighbors property is ignored " +
                    "because topology has no enough nodes to assign backups.");

                exclNeighborsWarn = true;
            }
        }

        assert res.size() <= primaryAndBackups;

        return res;
    }

    /**
     * Creates assignment for REPLICATED cache
     *
     * @param nodes Topology.
     * @param sortedNodes Sorted for specified partitions nodes.
     * @return Assignment.
     */
    private List<NetworkMember> replicatedAssign(List<NetworkMember> nodes, Iterable<NetworkMember> sortedNodes) {
        NetworkMember primary = sortedNodes.iterator().next();

        List<NetworkMember> res = new ArrayList<>(nodes.size());

        res.add(primary);

        for (NetworkMember n : nodes)
            if (!n.equals(primary))
                res.add(n);

        assert res.size() == nodes.size() : "Not enough backups: " + res.size();

        return res;
    }

    /**
     * The pack partition number and nodeHash.hashCode to long and mix it by hash function based on the Wang/Jenkins
     * hash.
     *
     * @param key0 Hash key.
     * @param key1 Hash key.
     * @see <a href="https://gist.github.com/badboy/6267743#64-bit-mix-functions">64 bit mix functions</a>
     * @return Long hash key.
     */
    private static long hash(int key0, int key1) {
        long key = (key0 & 0xFFFFFFFFL)
            | ((key1 & 0xFFFFFFFFL) << 32);

        key = (~key) + (key << 21); // key = (key << 21) - key - 1;
        key ^= (key >>> 24);
        key += (key << 3) + (key << 8); // key * 265
        key ^= (key >>> 14);
        key += (key << 2) + (key << 4); // key * 21
        key ^= (key >>> 28);
        key += (key << 31);

        return key;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return parts;
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key) {
        if (key == null)
            throw new IllegalArgumentException("Null key is passed for a partition calculation. " +
                "Make sure that an affinity key that is used is initialized properly.");

        return calculatePartition(key, mask, parts);
    }

    /** {@inheritDoc} */
    @Override public List<List<NetworkMember>> assignPartitions(Collection<NetworkMember> currentTopologySnapshot, int backups) {
        List<List<NetworkMember>> assignments = new ArrayList<>(parts);

        Map<UUID, Collection<NetworkMember>> neighborhoodCache = exclNeighbors ?
            neighbors(currentTopologySnapshot) : null;

        List<NetworkMember> nodes = new ArrayList<>(currentTopologySnapshot);

        for (int i = 0; i < parts; i++) {
            List<NetworkMember> partAssignment = assignPartition(i, nodes, backups, neighborhoodCache);

            assignments.add(partAssignment);
        }

        return assignments;
    }

    /**
     * Builds neighborhood map for all nodes in snapshot.
     *
     * @param topSnapshot Topology snapshot.
     * @return Neighbors map.
     */
    public static Map<UUID, Collection<NetworkMember>> neighbors(Collection<NetworkMember> topSnapshot) {
        Map<String, Collection<NetworkMember>> macMap = new HashMap<>(topSnapshot.size(), 1.0f);

        // Group by mac addresses.
        for (NetworkMember node : topSnapshot) {
            String macs = String.valueOf(node.hashCode());
                //node.attribute(IgniteNodeAttributes.ATTR_MACS);

            Collection<NetworkMember> nodes = macMap.get(macs);

            if (nodes == null)
                macMap.put(macs, nodes = new HashSet<>());

            nodes.add(node);
        }

        Map<UUID, Collection<NetworkMember>> neighbors = new HashMap<>(topSnapshot.size(), 1.0f);

        for (Collection<NetworkMember> group : macMap.values())
            for (NetworkMember node : group)
                neighbors.put(node.id(), group);

        return neighbors;
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        // No-op.
    }

    /**
     *
     */
    private static class HashComparator implements Comparator<IgniteBiTuple<Long, NetworkMember>>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public int compare(IgniteBiTuple<Long, NetworkMember> o1, IgniteBiTuple<Long, NetworkMember> o2) {
            return o1.get1() < o2.get1() ? -1 : o1.get1() > o2.get1() ? 1 :
                o1.get2().id().compareTo(o2.get2().id());
        }
    }

    /**
     * Sorts the initial array with linear sort algorithm array
     */
    private static class LazyLinearSortedContainer implements Iterable<NetworkMember> {
        /** Initial node-hash array. */
        private final IgniteBiTuple<Long, NetworkMember>[] arr;

        /** Count of the sorted elements */
        private int sorted;

        /**
         * @param arr Node / partition hash list.
         * @param needFirstSortedCnt Estimate count of elements to return by iterator.
         */
        LazyLinearSortedContainer(IgniteBiTuple<Long, NetworkMember>[] arr, int needFirstSortedCnt) {
            this.arr = arr;

            if (needFirstSortedCnt > (int)Math.log(arr.length)) {
                Arrays.sort(arr, COMPARATOR);

                sorted = arr.length;
            }
        }

        /** {@inheritDoc} */
        @Override public Iterator<NetworkMember> iterator() {
            return new SortIterator();
        }

        /**
         *
         */
        private class SortIterator implements Iterator<NetworkMember> {
            /** Index of the first unsorted element. */
            private int cur;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return cur < arr.length;
            }

            /** {@inheritDoc} */
            @Override public NetworkMember next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                if (cur < sorted)
                    return arr[cur++].get2();

                IgniteBiTuple<Long, NetworkMember> min = arr[cur];

                int minIdx = cur;

                for (int i = cur + 1; i < arr.length; i++) {
                    if (COMPARATOR.compare(arr[i], min) < 0) {
                        minIdx = i;

                        min = arr[i];
                    }
                }

                if (minIdx != cur) {
                    arr[minIdx] = arr[cur];

                    arr[cur] = min;
                }

                sorted = cur++;

                return min.get2();
            }

            /** {@inheritDoc} */
            @Override public void remove() {
                throw new UnsupportedOperationException("Remove doesn't supported");
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "U.toString(RendezvousAffinityFunction.class, this)";
    }

    /**
     * Gets absolute value for integer. If integer is {@link Integer#MIN_VALUE}, then {@code 0} is returned.
     *
     * @param i Integer.
     * @return Absolute value.
     */
    public static int safeAbs(int i) {
        i = Math.abs(i);

        return i < 0 ? 0 : i;
    }
}
