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

package org.apache.ignite.internal.table.distributed.replicator;

/**
 * Key for table definitions diff.
 */
class TableDefinitionDiffKey {
    private final int tableId;
    private final int fromSchemaVersion;
    private final int toSchemaVersion;

    TableDefinitionDiffKey(int tableId, int fromSchemaVersion, int toSchemaVersion) {
        this.tableId = tableId;
        this.fromSchemaVersion = fromSchemaVersion;
        this.toSchemaVersion = toSchemaVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TableDefinitionDiffKey that = (TableDefinitionDiffKey) o;

        if (tableId != that.tableId) {
            return false;
        }
        if (fromSchemaVersion != that.fromSchemaVersion) {
            return false;
        }
        return toSchemaVersion == that.toSchemaVersion;
    }

    @Override
    public int hashCode() {
        int result = tableId;
        result = 31 * result + fromSchemaVersion;
        result = 31 * result + toSchemaVersion;
        return result;
    }
}
