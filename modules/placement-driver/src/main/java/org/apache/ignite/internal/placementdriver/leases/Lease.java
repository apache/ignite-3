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

import static org.apache.ignite.internal.hlc.HybridTimestamp.HYBRID_TIMESTAMP_SIZE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.MIN_VALUE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ByteUtils;

/**
 * A lease representation in memory.
 * The real lease is stored in Meta storage.
 */
public class Lease implements ReplicaMeta {
    /** The object is used when nothing holds the lease. Empty lease is always expired. */
    public static Lease EMPTY_LEASE = new Lease(null, MIN_VALUE, MIN_VALUE, null);

    /** A node that holds a lease. */
    private final String leaseholder;

    /** The lease is accepted, when the holder knows about it and applies all related obligations. */
    private final boolean accepted;

    /** Lease start timestamp. The timestamp is assigned when the lease created and is not changed when the lease is prolonged. */
    private final HybridTimestamp startTime;

    /** Timestamp to expiration the lease. */
    private final HybridTimestamp expirationTime;

    /** The lease is available to prolong in the same leaseholder. */
    private final boolean prolongable;

    /** Id of replication group. */
    private final ReplicationGroupId replicationGroupId;

    /**
     * Creates a new lease.
     *
     * @param leaseholder Lease holder.
     * @param startTime Start lease timestamp.
     * @param leaseExpirationTime Lease expiration timestamp.
     * @param replicationGroupId Id of replication group.
     */
    public Lease(
            String leaseholder,
            HybridTimestamp startTime,
            HybridTimestamp leaseExpirationTime,
            ReplicationGroupId replicationGroupId
    ) {
        this(leaseholder, startTime, leaseExpirationTime, false, false, replicationGroupId);
    }

    /**
     * The constructor.
     *
     * @param leaseholder Lease holder.
     * @param startTime Start lease timestamp.
     * @param leaseExpirationTime Lease expiration timestamp.
     * @param prolong Lease is available to prolong.
     * @param accepted The flag is true when the holder accepted the lease, the false otherwise.
     */
    public Lease(
            String leaseholder,
            HybridTimestamp startTime,
            HybridTimestamp leaseExpirationTime,
            boolean prolong,
            boolean accepted,
            ReplicationGroupId replicationGroupId
    ) {
        this.leaseholder = leaseholder;
        this.startTime = startTime;
        this.expirationTime = leaseExpirationTime;
        this.prolongable = prolong;
        this.accepted = accepted;
        this.replicationGroupId = replicationGroupId;
    }

    /**
     * Prolongs a lease until new timestamp. Only an accepted lease can be prolonged.
     *
     * @param to The new lease expiration timestamp.
     * @return A new lease which will have the same properties except of expiration timestamp.
     */
    public Lease prolongLease(HybridTimestamp to) {
        assert accepted : "The lease should be accepted by leaseholder before prolongation ["
                + "leaseholder=" + leaseholder
                + ", expirationTime=" + expirationTime
                + ", prolongTo=" + to + ']';

        assert prolongable : "The lease should be available to prolong ["
                + "leaseholder=" + leaseholder
                + ", expirationTime=" + expirationTime
                + ", prolongTo=" + to + ']';

        return new Lease(leaseholder, startTime, to, true, true, replicationGroupId);
    }

    /**
     * Accepts the lease.
     *
     * @param to The new lease expiration timestamp.
     * @return A accepted lease.
     */
    public Lease acceptLease(HybridTimestamp to) {
        assert !accepted : "The lease is already accepted ["
                + "leaseholder=" + leaseholder
                + ", expirationTime=" + expirationTime + ']';

        return new Lease(leaseholder, startTime, to, true, true, replicationGroupId);
    }

    /**
     * Denies the lease.
     *
     * @return Denied lease.
     */
    public Lease denyLease() {
        assert accepted : "The lease is not accepted ["
                + "leaseholder=" + leaseholder
                + ", expirationTime=" + expirationTime + ']';

        return new Lease(leaseholder, startTime, expirationTime, false, true, replicationGroupId);
    }

    @Override
    public String getLeaseholder() {
        return leaseholder;
    }

    @Override
    public HybridTimestamp getStartTime() {
        return startTime;
    }

    @Override
    public HybridTimestamp getExpirationTime() {
        return expirationTime;
    }

    /**
     * Gets a prolongation flag.
     *
     * @return True if the lease might be prolonged, false otherwise.
     */
    public boolean isProlongable() {
        return prolongable;
    }

    /**
     * Gets accepted flag.
     *
     * @return True if the lease accepted, false otherwise.
     */
    public boolean isAccepted() {
        return accepted;
    }

    public ReplicationGroupId replicationGroupId() {
        return replicationGroupId;
    }

    /**
     * Encodes this lease into sequence of bytes.
     *
     * @return Lease representation in a byte array.
     */
    public byte[] bytes() {
        byte[] leaseholderBytes = leaseholder == null ? null : leaseholder.getBytes(StandardCharsets.UTF_8);
        short leaseholderBytesSize = (short) (leaseholderBytes == null ? 0 : leaseholderBytes.length);

        byte[] groupIdBytes = replicationGroupId == null ? null : ByteUtils.toBytes(replicationGroupId);
        short groupIdBytesSize = (short) (groupIdBytes == null ? 0 : groupIdBytes.length);

        int bufSize = leaseholderBytesSize + groupIdBytesSize + Short.BYTES * 2 + HYBRID_TIMESTAMP_SIZE * 2 + 1 + 1;

        ByteBuffer buf = ByteBuffer.allocate(bufSize).order(ByteOrder.LITTLE_ENDIAN);

        buf.put((byte) (accepted ? 1 : 0));
        buf.put((byte) (prolongable ? 1 : 0));
        buf.putLong(startTime.longValue());
        buf.putLong(expirationTime.longValue());
        buf.putShort(leaseholderBytesSize);
        if (leaseholderBytes != null) {
            buf.put(leaseholderBytes);
        }
        buf.putShort(groupIdBytesSize);
        if (groupIdBytes != null) {
            buf.put(groupIdBytes);
        }

        return buf.array();
    }

    /**
     * Decodes a lease from the sequence of bytes.
     *
     * @param buf Byte buffer containing lease representation. Requires to be in little-endian.
     * @return Decoded lease.
     */
    public static Lease fromBytes(ByteBuffer buf) {
        boolean accepted = buf.get() == 1;
        boolean prolongable = buf.get() == 1;
        HybridTimestamp startTime = hybridTimestamp(buf.getLong());
        HybridTimestamp expirationTime = hybridTimestamp(buf.getLong());
        short leaseholderBytesSize = buf.getShort();
        String leaseholder;
        if (leaseholderBytesSize > 0) {
            byte[] leaseholderBytes = new byte[leaseholderBytesSize];
            buf.get(leaseholderBytes);
            leaseholder = new String(leaseholderBytes, StandardCharsets.UTF_8);
        } else {
            leaseholder = null;
        }

        short groupIdBytesSize = buf.getShort();
        ReplicationGroupId groupId;
        if (groupIdBytesSize > 0) {
            byte[] groupIdBytes = new byte[groupIdBytesSize];
            buf.get(groupIdBytes);
            groupId = ByteUtils.fromBytes(groupIdBytes);
        } else {
            groupId = null;
        }

        return new Lease(leaseholder, startTime, expirationTime, prolongable, accepted, groupId);
    }

    @Override
    public String toString() {
        return S.toString(Lease.class, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Lease lease = (Lease) o;
        return accepted == lease.accepted && prolongable == lease.prolongable && Objects.equals(leaseholder, lease.leaseholder)
                && Objects.equals(startTime, lease.startTime) && Objects.equals(expirationTime, lease.expirationTime)
                && Objects.equals(replicationGroupId, lease.replicationGroupId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaseholder, accepted, startTime, expirationTime, prolongable, replicationGroupId);
    }
}
