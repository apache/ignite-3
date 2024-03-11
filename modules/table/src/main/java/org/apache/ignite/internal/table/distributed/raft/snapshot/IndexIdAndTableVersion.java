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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import org.apache.ignite.internal.tostring.S;

/** Container for index ID and table schema version at index creation. */
public class IndexIdAndTableVersion {
    private final int indexId;

    private final int tableVersion;

    /** Constructor. */
    public IndexIdAndTableVersion(int indexId, int tableVersion) {
        this.indexId = indexId;
        this.tableVersion = tableVersion;
    }

    /** Returns index ID. */
    public int indexId() {
        return indexId;
    }

    /** Returns the table schema version at the time the index was created. */
    public int tableVersion() {
        return tableVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexIdAndTableVersion other = (IndexIdAndTableVersion) o;

        return indexId == other.indexId && tableVersion == other.tableVersion;
    }

    @Override
    public int hashCode() {
        int result = indexId;
        result = 31 * result + tableVersion;
        return result;
    }

    @Override
    public String toString() {
        return S.toString(IndexIdAndTableVersion.class, this);
    }
}
