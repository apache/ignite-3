/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.placementdriver.leases;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;
import org.jetbrains.annotations.Nullable;

/**
 * {@link VersionedSerializer} for {@link LeaseBatch} instances.
 *
 * <p>Format grammar:
 * <pre>{@code
 *     <BATCH> ::= <HEADER> <TABLE_LEASES_SECTION>
 *
 *     <HEADER> ::=
 *       <MIN_EXPIRATION_TIME_PHYSICAL_PART> (varint) // Self-explanatory
 *       <COMMON_EXPIRATION_TIME_PHYSICAL_PART_DELTA> (varint) // Delta between physical part of common expiration time and min exp. time
 *       <COMMON_EXPIRATION_TIME_LOGICAL_PART> (varint) // Logical part of most common expiration time
 *       <NODE_DICTIONARY>
 *
 *     <NODE_DICTIONARY> ::=
 *       <NAME_COUNT> (varint)
 *       { <NODE_NAME> (UTF) } (nameCount times)
 *       <NODE_COUNT> (varint)
 *       {
 *         <NODE_ID> (UUID)
 *         <NODE_NAME_INDEX> (varint) // Index in the name table (defined above)
 *       } (nodeCount times)
 *
 *     <TABLE_LEASES_SECTION> ::=
 *       <TABLE_COUNT> (varint)
 *       { <OBJECT_LEASES> } // tableCount of OBJECT_LEASES elements
 *
 *     <OBJECT_LEASES> ::=
 *       <OBJECT_ID_DELTA> (varint) // For first object in section, it's full object ID; for subsequent once, it's object ID minus
 *                                  // previous object ID
 *       <LEASE_COUNT> (varint) // Number of leases (including holes) for current object
 *       { <LEASE> } // leaseCount times
 *
 *     <LEASE> ::=
 *       <FLAGS> (byte)
 *       [ // Only present if DUMMY_LEASE flag is 0
 *         <HOLDER_AND_PROPOSED_CANDIDATE>
 *         [ <EXPIRATION_TIME> ] // Only written if HAS_UNCOMMON_EXPIRATION_TIME flag is 1
 *         <START_TIME>
 *       ]
 *
 *       <HOLDER_AND_PROPOSED_CANDIDATE> ::=
 *         <HOLDER_AND_PROPOSED_CANDIDATE_COMPACTLY> (byte) // Only if number of node names in the dictionary is 8 or less; lowest 3 bits
 *                                                          // encode holder index (in the nodes table), and next 3 bits are for proposed
 *                                                          // candidate index (in the names table)
 *         | <HOLDER_INDEX> (varint) [ <PROPOSED_CANDIDATE> (varint) ] // Proposed candidate is only written if HAS_PROPOSED_CANDIDATE
 *                                                                     // flag is 1
 *       <EXPIRATION_TIME> ::=
 *         <EXPIRATION_TIME_PHYSICAL_PART_DELTA> (varint) // Relative to minExpirationTimePhysicalPart
 *         [ EXPIRATION_TIME_LOGICAL_PART ] (varint) // Only written if HAS_EXPIRATION_TIME_LOGICAL flag is 1
 *
 *       <START_TIME> ::=
 *         <PERIOD> // Difference between physical parts of expirationTime and startTime
 *         <START_TIME_LOGICAL_PART (varint)
 * }</pre>
 *
 * <p>The following optimizations are applied to minimize the amount of bytes a batch is serialized to:</p>
 * <ul>
 *     <li>Java Serialization is not used to serialize components (it's pretty verbose)</li>
 *     <li>Varints are used extensively</li>
 *     <li>A dictionary of all nodes mentioned as lease holders and proposed candidates is collected and written in the header once
 *     per batch. This is beneficial as we usually a lot more leases than number of nodes in cluster, so we can just represent nodes
 *     and their names as indices in the dictionary (this is especially effective given we use varints as indices are usually very small).
 *     </li>
 *     <li>Leases are grouped per table/zone ID (aka object ID), so an object ID is only written once per table </li>
 * </ul>
 */
public class LeaseBatchSerializer extends VersionedSerializer<LeaseBatch> {
    /** Serializer instance. */
    public static final LeaseBatchSerializer INSTANCE = new LeaseBatchSerializer();

    /** Contains {@link Lease#isAccepted()}. */
    @SuppressWarnings("PointlessBitwiseExpression")
    private static final int ACCEPTED_MASK = 1 << 0;

    /** Contains {@link Lease#isProlongable()}. */
    private static final int PROLONGABLE_MASK = 1 << 1;

    /** Whether the lease has a non-null {@link Lease#proposedCandidate()}. */
    private static final int HAS_PROPOSED_CANDIDATE_MASK = 1 << 2;

    /** Whether expiration time differs from the most common expiration time in the batch. */
    private static final int HAS_UNCOMMON_EXPIRATION_TIME_MASK = 1 << 3;

    /** Whether expiration timestamp logical part is not zero (this is uncommon). */
    private static final int HAS_EXPIRATION_LOGICAL_PART_MASK = 1 << 4;

    /** Whether this is not a real lease, but a hole in partitionId sequence. Having this flag allows us to omit partitionId. */
    private static final int DUMMY_LEASE_MASK = 1 << 5;

    // When there are no more 8 nodes in the cluster, node name index and node index are guaranteed to fit in 7 bits we have in a varint
    // byte, which allows us to enable 'compact mode' to save 1 byte per lease.

    /** Number of bits to fit name index/node index in to enable compact mode. */
    private static final int BIT_WIDTH_TO_FIT_IN_HALF_BYTE = 3;

    /** Max size of cluster which allows compact mode. */
    private static final int MAX_NODES_FOR_COMPACT_MODE = 1 << BIT_WIDTH_TO_FIT_IN_HALF_BYTE;

    /** Mask to extract lease holder index from compact representation. */
    private static final int COMPACT_HOLDER_INDEX_MASK = (1 << BIT_WIDTH_TO_FIT_IN_HALF_BYTE) - 1;

    @Override
    protected void writeExternalData(LeaseBatch batch, IgniteDataOutput out) throws IOException {
        long minExpirationTimePhysical = minExpirationTimePhysicalPart(batch);
        HybridTimestamp commonExpirationTime = mostFrequentExpirationTime(batch);

        out.writeVarInt(minExpirationTimePhysical);
        out.writeVarInt(commonExpirationTime.getPhysical() - minExpirationTimePhysical);
        out.writeVarInt(commonExpirationTime.getLogical());

        NodesDictionary nodesDictionary = buildNodesDictionary(batch);
        nodesDictionary.writeTo(out);

        List<Lease> tableLeases = batch.leases().stream()
                .filter(lease -> lease.replicationGroupId() instanceof TablePartitionId)
                .collect(toList());
        List<Lease> zoneLeases = batch.leases().stream()
                .filter(lease -> lease.replicationGroupId() instanceof ZonePartitionId)
                .collect(toList());
        assert tableLeases.size() + zoneLeases.size() == batch.leases().size() : "There are " + batch.leases().size()
                + " leases in total, "
                + tableLeases.size() + " of them are table leases, " + zoneLeases.size() + " are zone leases, but "
                + (batch.leases().size() - tableLeases.size() - zoneLeases.size()) + " are neither";

        writePartitionedGroupLeases(tableLeases, minExpirationTimePhysical, commonExpirationTime, nodesDictionary, out);

        writePartitionedGroupLeases(zoneLeases, minExpirationTimePhysical, commonExpirationTime, nodesDictionary, out);
    }

    private static long minExpirationTimePhysicalPart(LeaseBatch batch) {
        long min = HybridTimestamp.MAX_VALUE.getPhysical();

        for (Lease lease : batch.leases()) {
            min = Math.min(min, lease.getExpirationTime().getPhysical());
        }

        return min;
    }

    private static HybridTimestamp mostFrequentExpirationTime(LeaseBatch batch) {
        if (batch.leases().isEmpty()) {
            return HybridTimestamp.MIN_VALUE;
        }

        Object2IntMap<HybridTimestamp> counts = new Object2IntOpenHashMap<>();

        for (Lease lease : batch.leases()) {
            counts.mergeInt(lease.getExpirationTime(), 1, Integer::sum);
        }

        HybridTimestamp commonExpirationTime = HybridTimestamp.MIN_VALUE;
        int maxCount = -1;
        for (Object2IntMap.Entry<HybridTimestamp> entry : counts.object2IntEntrySet()) {
            if (entry.getIntValue() > maxCount) {
                commonExpirationTime = entry.getKey();
                maxCount = entry.getIntValue();
            }
        }

        return commonExpirationTime;
    }

    private static NodesDictionary buildNodesDictionary(LeaseBatch batch) {
        NodesDictionary nodesDictionary = new NodesDictionary();

        for (Lease lease : batch.leases()) {
            if (lease.getLeaseholderId() != null) {
                assert lease.getLeaseholder() != null : lease;
                nodesDictionary.putNode(lease.getLeaseholderId(), lease.getLeaseholder());
            }
            if (lease.proposedCandidate() != null) {
                //noinspection DataFlowIssue
                nodesDictionary.putName(lease.proposedCandidate());
            }
        }

        return nodesDictionary;
    }

    private static void writePartitionedGroupLeases(
            List<Lease> leases,
            long minExpirationTimePhysical,
            HybridTimestamp commonExpirationTime,
            NodesDictionary nodesDictionary,
            IgniteDataOutput out
    ) throws IOException {
        Map<Integer, List<Lease>> leasesByObjectId = leases.stream()
                .collect(
                        groupingBy(
                                lease -> partitionedGroupIdFrom(lease).objectId(),
                                TreeMap::new,
                                toList()
                        )
                );

        out.writeVarInt(leasesByObjectId.size());

        int objectIdBase = 0;
        for (Entry<Integer, List<Lease>> entry : leasesByObjectId.entrySet()) {
            int objectId = entry.getKey();
            List<Lease> objectLeases = entry.getValue();

            objectIdBase = writeLeasesForObject(
                    objectId,
                    objectLeases,
                    minExpirationTimePhysical,
                    commonExpirationTime,
                    nodesDictionary,
                    out,
                    objectIdBase
            );
        }
    }

    private static PartitionGroupId partitionedGroupIdFrom(Lease lease) {
        return (PartitionGroupId) lease.replicationGroupId();
    }

    private static int writeLeasesForObject(
            int objectId,
            List<Lease> objectLeases,
            long minExpirationTimePhysical,
            HybridTimestamp commonExpirationTime,
            NodesDictionary nodesDictionary,
            IgniteDataOutput out,
            int objectIdBase
    ) throws IOException {
        objectLeases.sort(comparing(LeaseBatchSerializer::partitionedGroupIdFrom, comparing(PartitionGroupId::partitionId)));

        out.writeVarInt(objectId - objectIdBase);

        int partitionCount = partitionedGroupIdFrom(objectLeases.get(objectLeases.size() - 1)).partitionId() + 1;
        out.writeVarInt(partitionCount);

        int partitionId = 0;
        for (Lease lease : objectLeases) {
            partitionId = writeLease(lease, partitionId, minExpirationTimePhysical, commonExpirationTime, nodesDictionary, out);
        }

        return objectId;
    }

    private static int writeLease(
            Lease lease,
            int partitionId,
            long minExpirationTimePhysical,
            HybridTimestamp commonExpirationTime,
            NodesDictionary nodesDictionary,
            IgniteDataOutput out
    ) throws IOException {
        PartitionGroupId groupId = partitionedGroupIdFrom(lease);

        while (partitionId < groupId.partitionId()) {
            // It's a hole in partitionId sequence, let's write a 'dummy lease'.
            out.write(DUMMY_LEASE_MASK);
            partitionId++;
        }

        assert partitionId == groupId.partitionId() : "Duplicate partitionId in " + lease;

        assert lease.getLeaseholder() != null && lease.getLeaseholderId() != null : lease + " doesn't have a leaseholder";
        assert lease.getStartTime() != HybridTimestamp.MIN_VALUE : lease + " has illegal start time";
        assert lease.getExpirationTime() != HybridTimestamp.MIN_VALUE : lease + " has illegal expiration time";

        UUID leaseHolderId = lease.getLeaseholderId();
        String proposedCandidate = lease.proposedCandidate();
        boolean hasProposedCandidate = proposedCandidate != null;

        boolean hasUncommonExpirationTime = !Objects.equals(lease.getExpirationTime(), commonExpirationTime);
        boolean hasExpirationLogicalPart = lease.getExpirationTime().getLogical() != 0;

        out.write(flags(
                lease.isAccepted(),
                lease.isProlongable(),
                hasProposedCandidate,
                hasUncommonExpirationTime,
                hasExpirationLogicalPart
        ));

        if (holderIdAndProposedCandidateFitIn1Byte(nodesDictionary)) {
            int nodesInfo = packNodesInfo(
                    nodesDictionary.getNodeIndex(leaseHolderId),
                    hasProposedCandidate ? nodesDictionary.getNameIndex(proposedCandidate) : 0
            );
            out.writeVarInt(nodesInfo);
        } else {
            out.writeVarInt(nodesDictionary.getNodeIndex(leaseHolderId));
            if (hasProposedCandidate) {
                out.writeVarInt(nodesDictionary.getNameIndex(proposedCandidate));
            }
        }

        if (hasUncommonExpirationTime) {
            out.writeVarInt(lease.getExpirationTime().getPhysical() - minExpirationTimePhysical);
            if (hasExpirationLogicalPart) {
                out.writeVarInt(lease.getExpirationTime().getLogical());
            }
        }

        long periodIn = lease.getExpirationTime().getPhysical() - lease.getStartTime().getPhysical();
        out.writeVarInt(periodIn);
        out.writeVarInt(lease.getStartTime().getLogical());

        return partitionId + 1;
    }

    private static int packNodesInfo(int holderNodeIndex, int proposedCandidateNameIndex) {
        assert holderNodeIndex < MAX_NODES_FOR_COMPACT_MODE : holderNodeIndex;
        assert proposedCandidateNameIndex < MAX_NODES_FOR_COMPACT_MODE : proposedCandidateNameIndex;

        return holderNodeIndex | (proposedCandidateNameIndex << BIT_WIDTH_TO_FIT_IN_HALF_BYTE);
    }

    private static boolean holderIdAndProposedCandidateFitIn1Byte(NodesDictionary dictionary) {
        // Up to 8 names means that for name index it's enough to have 3 bits, same for node index, so, in sum, they
        // require up to 6 bits, and we have 7 bits in a varint byte.
        return dictionary.nameCount() <= MAX_NODES_FOR_COMPACT_MODE;
    }

    private static int flags(
            boolean accepted,
            boolean prolongable,
            boolean hasProposedCandidate,
            boolean hasUncommonExpirationTime,
            boolean hasExpirationLogicalPart
    ) {
        return (accepted ? ACCEPTED_MASK : 0)
                | (prolongable ? PROLONGABLE_MASK : 0)
                | (hasProposedCandidate ? HAS_PROPOSED_CANDIDATE_MASK : 0)
                | (hasUncommonExpirationTime ? HAS_UNCOMMON_EXPIRATION_TIME_MASK : 0)
                | (hasExpirationLogicalPart ? HAS_EXPIRATION_LOGICAL_PART_MASK : 0);
    }

    @Override
    protected LeaseBatch readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        long minExpirationTimePhysical = in.readVarInt();
        HybridTimestamp commonExpirationTime = new HybridTimestamp(minExpirationTimePhysical + in.readVarInt(), in.readVarIntAsInt());
        NodesDictionary nodesDictionary = NodesDictionary.readFrom(in);

        List<Lease> leases = new ArrayList<>();

        readPartitionedGroupLeases(
                minExpirationTimePhysical,
                commonExpirationTime,
                nodesDictionary,
                leases,
                in,
                TablePartitionId::new
        );

        if (in.available() > 0) {
            readPartitionedGroupLeases(
                    minExpirationTimePhysical,
                    commonExpirationTime,
                    nodesDictionary,
                    leases,
                    in,
                    ZonePartitionId::new
            );
        }

        return new LeaseBatch(leases);
    }

    private static void readPartitionedGroupLeases(
            long minExpirationTimePhysical,
            HybridTimestamp commonExpirationTime,
            NodesDictionary nodesDictionary,
            List<Lease> leases,
            IgniteDataInput in,
            GroupIdFactory groupIdFactory
    ) throws IOException {
        int objectCount = in.readVarIntAsInt();

        int objectIdBase = 0;
        for (int i = 0; i < objectCount; i++) {
            objectIdBase = readLeasesForObject(
                    minExpirationTimePhysical,
                    commonExpirationTime,
                    nodesDictionary,
                    leases,
                    in,
                    groupIdFactory,
                    objectIdBase
            );
        }
    }

    private static int readLeasesForObject(
            long minExpirationTimePhysical,
            HybridTimestamp commonExpirationTime,
            NodesDictionary nodesDictionary,
            List<Lease> leases,
            IgniteDataInput in,
            GroupIdFactory groupIdFactory,
            int objectIdBase
    ) throws IOException {
        int objectId = objectIdBase + in.readVarIntAsInt();

        int partitionCount = in.readVarIntAsInt();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            Lease lease = readLeaseForPartition(
                    partitionId,
                    objectId,
                    minExpirationTimePhysical,
                    commonExpirationTime,
                    nodesDictionary,
                    in,
                    groupIdFactory
            );
            if (lease != null) {
                leases.add(lease);
            }
        }

        return objectId;
    }

    private static @Nullable Lease readLeaseForPartition(
            int partitionId,
            int objectId,
            long minExpirationTimePhysical,
            HybridTimestamp commonExpirationTime,
            NodesDictionary nodesDictionary,
            IgniteDataInput in,
            GroupIdFactory groupIdFactory
    ) throws IOException {
        int flags = in.read();
        if (flagSet(flags, DUMMY_LEASE_MASK)) {
            // This represents a hole, just skip it.
            return null;
        }

        boolean hasProposedCandidate = flagSet(flags, HAS_PROPOSED_CANDIDATE_MASK);

        int holderNodeIndex;
        int proposedCandidateNodeIndex = -1;
        if (holderIdAndProposedCandidateFitIn1Byte(nodesDictionary)) {
            int nodesInfo = in.readVarIntAsInt();

            holderNodeIndex = unpackHolderNodeIndex(nodesInfo);

            if (hasProposedCandidate) {
                proposedCandidateNodeIndex = unpackProposedCandidateNameIndex(nodesInfo);
            }
        } else {
            holderNodeIndex = in.readVarIntAsInt();

            if (hasProposedCandidate) {
                proposedCandidateNodeIndex = in.readVarIntAsInt();
            }
        }

        UUID leaseHolderId = nodesDictionary.getNodeId(holderNodeIndex);
        String leaseHolder = nodesDictionary.getNodeName(holderNodeIndex);
        String proposedCandidate = null;
        if (hasProposedCandidate) {
            proposedCandidate = nodesDictionary.getName(proposedCandidateNodeIndex);
        }

        HybridTimestamp expirationTime;
        if (flagSet(flags, HAS_UNCOMMON_EXPIRATION_TIME_MASK)) {
            long expirationPhysical = minExpirationTimePhysical + in.readVarInt();
            int expirationLogical = flagSet(flags, HAS_EXPIRATION_LOGICAL_PART_MASK) ? in.readVarIntAsInt() : 0;
            expirationTime = new HybridTimestamp(expirationPhysical, expirationLogical);
        } else {
            expirationTime = commonExpirationTime;
        }

        long period = in.readVarInt();
        int startLogical = in.readVarIntAsInt();

        HybridTimestamp startTime = new HybridTimestamp(expirationTime.getPhysical() - period, startLogical);

        return new Lease(
                leaseHolder,
                leaseHolderId,
                startTime,
                expirationTime,
                flagSet(flags, PROLONGABLE_MASK),
                flagSet(flags, ACCEPTED_MASK),
                proposedCandidate,
                groupIdFactory.create(objectId, partitionId)
        );
    }

    private static int unpackHolderNodeIndex(int nodesInfo) {
        return nodesInfo & COMPACT_HOLDER_INDEX_MASK;
    }

    private static int unpackProposedCandidateNameIndex(int nodesInfo) {
        return nodesInfo >> BIT_WIDTH_TO_FIT_IN_HALF_BYTE;
    }

    private static boolean flagSet(int flags, int mask) {
        return (flags & mask) != 0;
    }

    @FunctionalInterface
    private interface GroupIdFactory {
        PartitionGroupId create(int objectId, int partitionId);
    }
}
