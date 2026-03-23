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

package org.apache.ignite.internal.jdbc;

import org.apache.ignite.internal.client.sql.ClientAsyncResultSet;
import org.apache.ignite.internal.sql.SyncResultSetAdapter;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;

/**
 * Implementation of {@link ClientSyncResultSet}.
 */
final class ClientSyncResultSetImpl implements ClientSyncResultSet {

    private final ClientAsyncResultSet<SqlRow> rs;

    private final SyncResultSetAdapter<SqlRow> syncRs;

    ClientSyncResultSetImpl(ClientAsyncResultSet<SqlRow> rs) {
        this.rs = rs;
        this.syncRs = new SyncResultSetAdapter<>(rs);
    }

    @Override
    public ResultSetMetadata metadata() {
        ResultSetMetadata metadata = syncRs.metadata();
        return metadata != null ? metadata : EMPTY_METADATA;
    }

    @Override
    public boolean hasRowSet() {
        return syncRs.hasRowSet();
    }

    @Override
    public long affectedRows() {
        return syncRs.affectedRows();
    }

    @Override
    public boolean hasNextResultSet() {
        return rs.hasNextResultSet();
    }

    @Override
    public ClientSyncResultSet nextResultSet() {
        if (!rs.hasNextResultSet()) {
            throw new IllegalStateException("Should not have been called.");
        }

        ClientAsyncResultSet<SqlRow> nextRs = rs.nextResultSet().join();
        return new ClientSyncResultSetImpl(nextRs);
    }

    @Override
    public void close() {
        syncRs.close();
    }

    @Override
    public boolean hasNext() {
        return syncRs.hasNext();
    }

    @Override
    public SqlRow next() {
        return syncRs.next();
    }
}
