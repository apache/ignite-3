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

package org.apache.ignite.internal.storage.pagememory.pending;

import java.util.UUID;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;

public class PendingRowsKey implements Comparable<PendingRowsKey> {
    @IgniteToStringInclude
    private final UUID transactionId;

    @IgniteToStringInclude
    private final RowId rowId;

    public PendingRowsKey(UUID transactionId, RowId rowId) {
        this.transactionId = transactionId;
        this.rowId = rowId;
    }

    public UUID transactionId() {
        return transactionId;
    }

    public RowId rowId() {
        return rowId;
    }

    @Override
    public int compareTo(PendingRowsKey o) {
        int cmp = transactionId.compareTo(o.transactionId);

        if (cmp != 0) {
            return cmp;
        }

        return rowId.compareTo(o.rowId);
    }

    @Override
    public int hashCode() {
        int result = transactionId.hashCode();

        result = 31 * result + rowId.hashCode();

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        PendingRowsKey other = (PendingRowsKey) obj;

        return transactionId.equals(other.transactionId) && rowId.equals(other.rowId);
    }
}
