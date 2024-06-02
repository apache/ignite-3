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

package org.apache.ignite.internal.datareplication.network.replication;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaRequest;
import org.apache.ignite.internal.datareplication.network.PartitionReplicationMessageGroup;

/**
 * Replica request to build a table index.
 *
 * <p>It is possible to receive a {@link PrimaryReplicaMissException} in response to message processing if the leaseholder changes.</p>
 */
@Transferable(PartitionReplicationMessageGroup.BUILD_INDEX_REPLICA_REQUEST)
public interface BuildIndexReplicaRequest extends PrimaryReplicaRequest {
    /** Returns index ID. */
    int indexId();

    /** Returns row IDs for which to build indexes. */
    List<UUID> rowIds();

    /** Returns {@code true} if this batch is the last one. */
    boolean finish();

    /** Return catalog version in which the index was created. */
    int creationCatalogVersion();
}
