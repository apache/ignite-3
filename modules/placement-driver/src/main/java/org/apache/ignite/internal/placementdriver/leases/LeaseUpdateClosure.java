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

import org.apache.ignite.internal.replicator.ReplicationGroupId;

/**
 * Closure to notify about a lease update.
 */
public interface LeaseUpdateClosure {

    /**
     * Notifies about lease update in Meta storage.
     * If the {@code oldLease} value is {@code null} the lease is newly created.
     * If the {@code newLease} value is {@code null} the lease is removed.
     *
     * @param groupId Replication grout id.
     * @param oldLease Lease of this replication group before the update.
     * @param newLease Lease of this replication group after the update.
     */
    void update(ReplicationGroupId groupId, Lease oldLease, Lease newLease);
}
