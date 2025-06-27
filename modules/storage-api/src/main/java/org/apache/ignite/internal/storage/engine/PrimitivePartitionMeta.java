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

import java.util.Objects;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.jetbrains.annotations.Nullable;

/**
 * Partition meta containing values of 'primitive' types (which are the same for all representations of partition metadata).
 */
public class PrimitivePartitionMeta {
    private final long lastAppliedIndex;
    private final long lastAppliedTerm;
    private final @Nullable LeaseInfo leaseInfo;

    /** Constructor. */
    public PrimitivePartitionMeta(
            long lastAppliedIndex,
            long lastAppliedTerm,
            @Nullable LeaseInfo leaseInfo
    ) {
        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
        this.leaseInfo = leaseInfo;
    }

    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    public long lastAppliedTerm() {
        return lastAppliedTerm;
    }

    public @Nullable LeaseInfo leaseInfo() {
        return leaseInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PrimitivePartitionMeta that = (PrimitivePartitionMeta) o;
        return lastAppliedIndex == that.lastAppliedIndex && lastAppliedTerm == that.lastAppliedTerm && Objects.equals(leaseInfo,
                that.leaseInfo);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(lastAppliedIndex);
        result = 31 * result + Long.hashCode(lastAppliedTerm);
        result = 31 * result + Objects.hashCode(leaseInfo);
        return result;
    }
}
