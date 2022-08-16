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

package org.apache.ignite.internal.table.distributed.replication.request;

import java.util.Collection;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.network.ClusterNode;

/**
 * Parameters for replica request.
 */
public class ReplicaRequestParameters {
    private InternalTransaction tx;

    private Collection<BinaryRow> binaryRows;

    private String groupId;

    private ClusterNode primaryReplica;

    private Long term;

    /**
     * The constructor.
     */
    public ReplicaRequestParameters(InternalTransaction tx, Collection<BinaryRow> binaryRows, String groupId, ClusterNode primaryReplica,
            Long term) {
        this.tx = tx;
        this.binaryRows = binaryRows;
        this.groupId = groupId;
        this.primaryReplica = primaryReplica;
        this.term = term;
    }

    /**
     * The constructor.
     */
    public ReplicaRequestParameters(InternalTransaction tx, String groupId, ClusterNode primaryReplica, Long term) {
        this.tx = tx;
        this.groupId = groupId;
        this.primaryReplica = primaryReplica;
        this.term = term;
    }

    public InternalTransaction tx() {
        return tx;
    }

    public Collection<BinaryRow> binaryRows() {
        return binaryRows;
    }

    public String groupId() {
        return groupId;
    }

    public ClusterNode replica() {
        return primaryReplica;
    }

    public Long term() {
        return term;
    }
}
