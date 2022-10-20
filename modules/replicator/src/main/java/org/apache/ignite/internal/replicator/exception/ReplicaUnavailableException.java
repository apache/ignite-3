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

package org.apache.ignite.internal.replicator.exception;

import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_UNAVAILABLE_ERR;

import java.util.UUID;
import org.apache.ignite.internal.replicator.message.ReplicationGroupId;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;

/**
 * The exception is thrown when a replica is not ready to handle a request.
 */
public class ReplicaUnavailableException extends IgniteInternalException {
    /**
     * The constructor.
     *
     * @param groupId Replication group id.
     * @param node Node.
     */
    public ReplicaUnavailableException(ReplicationGroupId groupId, ClusterNode node) {
        super(REPLICA_UNAVAILABLE_ERR, "Replica is not ready [replicationGroupId=" + groupId + ", nodeName=" + node.name() + ']');
    }

    /**
     * The constructor is used for creating an exception instance that is thrown from a remote server.
     *
     * @param traceId Trace id.
     * @param code Error code.
     * @param message Error message.
     * @param cause Cause exception.
     */
    public ReplicaUnavailableException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
