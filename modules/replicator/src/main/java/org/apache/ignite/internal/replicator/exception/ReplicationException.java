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

import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_COMMON_ERR;

import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.replicator.ReplicationGroupId;

/**
 * The exception is thrown when some issue happened during a replication.
 */
public class ReplicationException extends IgniteInternalException {
    /**
     * Constructor.
     *
     * @param replicaGrpId Replication group id.
     */
    public ReplicationException(ReplicationGroupId replicaGrpId) {
        this(replicaGrpId, null);
    }

    /**
     * The constructor.
     *
     * @param replicaGrpId Replication group id.
     * @param cause        Optional nested exception (can be {@code null}).
     */
    public ReplicationException(ReplicationGroupId replicaGrpId, Throwable cause) {
        this(REPLICA_COMMON_ERR, "Failed to process replica request [replicaGroupId=" + replicaGrpId + ']', cause);
    }

    /**
     * The constructor.
     *
     * @param code    Exception code.
     * @param message Exception message.
     */
    public ReplicationException(int code, String message) {
        super(code, message);
    }

    /**
     * The constructor.
     *
     * @param code    Exception code.
     * @param message Exception message.
     * @param cause   Optional nested exception (can be {@code null}).
     */
    public ReplicationException(int code, String message, Throwable cause) {
        super(code, message, cause);
    }

    /**
     * The constructor is used for creating an exception instance that is thrown from a remote server.
     *
     * @param traceId Trace id.
     * @param code    Error code.
     * @param message Error message.
     * @param cause   Cause exception.
     */
    public ReplicationException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
