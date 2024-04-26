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

package org.apache.ignite.table.partition;

import java.io.Serializable;
import org.apache.ignite.table.criteria.CriteriaVisitor;
import org.apache.ignite.table.criteria.Partition;
import org.jetbrains.annotations.Nullable;

/**
 * Hash partition representation.
 */
public class HashPartition implements Partition, Serializable {
    private static final long serialVersionUID = 1717320056615864614L;

    private final int partitionId;

    public HashPartition(int partitionId) {
        this.partitionId = partitionId;
    }

    public int partitionId() {
        return partitionId;
    }

    @Override
    public <C> void accept(CriteriaVisitor<C> v, @Nullable C context) {
        v.visit(this, context);
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
}
