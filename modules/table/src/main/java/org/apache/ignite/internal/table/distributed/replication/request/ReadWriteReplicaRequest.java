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

package org.apache.ignite.internal.table.distributed.replication.request;

import java.util.UUID;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaRequest;
import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.jetbrains.annotations.Nullable;

/**
 * Read-write replica request.
 */
public interface ReadWriteReplicaRequest extends PrimaryReplicaRequest, TimestampAware {
    UUID transactionId();

    /**
     * Gets a raft term.
     * TODO: A temp solution until lease-based engine will be implemented (IGNITE-17256, IGNITE-15083)
     *
     * @return Raft term.
     */
    @Deprecated
    @Nullable
    Long term();

    /**
     * Return {@code true} if this is a full transaction.
     */
    boolean full();
}
