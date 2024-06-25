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

package org.apache.ignite.internal.table.distributed.disaster;

import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

/**
 * Local partition state.
 */
public class LocalPartitionState {
    @IgniteToStringInclude
    public final String tableName;

    @IgniteToStringInclude
    public final String zoneName;

    @IgniteToStringInclude
    public final int partitionId;

    @IgniteToStringInclude
    public final LocalPartitionStateEnum state;

    LocalPartitionState(String tableName, String zoneName, int partitionId, LocalPartitionStateEnum state) {
        this.tableName = tableName;
        this.zoneName = zoneName;
        this.partitionId = partitionId;
        this.state = state;
    }

    @Override
    public String toString() {
        return S.toString(LocalPartitionState.class, this);
    }
}
