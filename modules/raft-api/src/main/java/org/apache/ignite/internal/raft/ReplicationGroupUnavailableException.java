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

package org.apache.ignite.internal.raft;

import static org.apache.ignite.lang.ErrorGroups.Replicator.GROUP_UNAVAILABLE_ERR;

import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.lang.IgniteException;

/**
 * Exception thrown by a Raft client when the replication group is unavailable.
 *
 * <p>This exception indicates that the replication group cannot process commands because:
 * <ul>
 *     <li>No leader is elected (no quorum available)</li>
 *     <li>The group is not yet formed</li>
 *     <li>The timeout for waiting for group availability has expired</li>
 * </ul>
 */
public class ReplicationGroupUnavailableException extends IgniteException {
    private static final long serialVersionUID = 3692064399873450610L;

    /** The replication group ID that is unavailable. */
    private final ReplicationGroupId groupId;

    /**
     * Constructor.
     *
     * @param groupId Replication group id.
     */
    public ReplicationGroupUnavailableException(ReplicationGroupId groupId) {
        super(GROUP_UNAVAILABLE_ERR, "Replication group is unavailable [groupId=" + groupId + "].");
        this.groupId = groupId;
    }

    /**
     * Constructor with custom message.
     *
     * @param groupId Replication group id.
     * @param message Custom message.
     */
    public ReplicationGroupUnavailableException(ReplicationGroupId groupId, String message) {
        super(GROUP_UNAVAILABLE_ERR, message);
        this.groupId = groupId;
    }

    /**
     * Constructor with cause.
     *
     * @param groupId Replication group id.
     * @param cause The cause of this exception.
     */
    public ReplicationGroupUnavailableException(ReplicationGroupId groupId, Throwable cause) {
        super(GROUP_UNAVAILABLE_ERR, "Replication group is unavailable [groupId=" + groupId + "].", cause);
        this.groupId = groupId;
    }

    /**
     * Returns the replication group ID that is unavailable.
     *
     * @return Replication group ID.
     */
    public ReplicationGroupId groupId() {
        return groupId;
    }
}
