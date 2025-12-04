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

package org.apache.ignite.internal.table.partition;

import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.table.partition.Partition;

/**
 * Hash partition representation.
 */
public class HashPartition implements Partition {
    private static final long serialVersionUID = 1717320056615864614L;

    private final int partitionId;

    public HashPartition(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public long id() {
        return partitionId;
    }

    @Deprecated(since = "3.2", forRemoval = true)
    public int partitionId() {
        return Math.toIntExact(id());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HashPartition that = (HashPartition) o;

        return partitionId == that.partitionId;
    }

    @Override
    public int hashCode() {
        return partitionId;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
