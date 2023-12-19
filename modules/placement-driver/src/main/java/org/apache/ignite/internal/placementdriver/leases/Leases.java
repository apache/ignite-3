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

import java.util.Map;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;

/** Leases received from the metastore. */
public class Leases {
    private final Map<ReplicationGroupId, Lease> leaseByGroupId;

    @IgniteToStringExclude
    private final byte[] leasesBytes;

    /**
     * The constructor.
     *
     * @param leaseByGroupId Leases by group map.
     * @param leasesBytes Serialized leases.
     */
    public Leases(Map<ReplicationGroupId, Lease> leaseByGroupId, byte[] leasesBytes) {
        assert leaseByGroupId != null;

        this.leaseByGroupId = leaseByGroupId;
        this.leasesBytes = leasesBytes;
    }

    /** Returns leases grouped by replication group. */
    public Map<ReplicationGroupId, Lease> leaseByGroupId() {
        return leaseByGroupId;
    }

    /** Returns an array of byte leases from the metastore. */
    public byte[] leasesBytes() {
        return leasesBytes;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
