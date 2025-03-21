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

package org.apache.ignite.internal.storage.engine;

import java.util.Arrays;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.jetbrains.annotations.Nullable;

/**
 * Partition metadata for {@link MvTableStorage#finishRebalancePartition(int, MvPartitionMeta)}.
 */
public class MvPartitionMeta extends PrimitivePartitionMeta {
    private final byte[] groupConfig;

    private final byte[] snapshotInfo;

    /** Constructor. */
    public MvPartitionMeta(
            long lastAppliedIndex,
            long lastAppliedTerm,
            byte[] groupConfig,
            @Nullable LeaseInfo leaseInfo,
            byte[] snapshotInfo
    ) {
        super(lastAppliedIndex, lastAppliedTerm, leaseInfo);

        this.groupConfig = groupConfig;
        this.snapshotInfo = snapshotInfo;
    }

    /** Returns replication group config as bytes. */
    public byte[] groupConfig() {
        return groupConfig;
    }

    /** Returns snapshot info as bytes. */
    public byte[] snapshotInfo() {
        return snapshotInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        MvPartitionMeta that = (MvPartitionMeta) o;
        return Arrays.equals(groupConfig, that.groupConfig) && Arrays.equals(snapshotInfo, that.snapshotInfo);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Arrays.hashCode(groupConfig);
        result = 31 * result + Arrays.hashCode(snapshotInfo);
        return result;
    }
}
