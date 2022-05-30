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

package org.apache.ignite.client.handler.requests.sql;

import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTx;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.client.handler.ClientResource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Session.SessionBuilder;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.Statement.StatementBuilder;

/**
 * Executes and SQL query.
 */
public class ClientSqlCursorNextPageRequest {
    /**
     * Processes the request.
     *
     * @param in  Unpacker.
     * @param out Packer.
     * @param sql SQL API.
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteSql sql,
            ClientResourceRegistry resources) {
        var tx = readTx(in, resources);

        SessionBuilder sessionBuilder = sql.sessionBuilder()
                .defaultPageSize(in.unpackInt())
                .defaultSchema(in.unpackString())
                .defaultTimeout(in.unpackLong(), TimeUnit.MILLISECONDS);

        var propCount = in.unpackInt();

        for (int i = 0; i < propCount; i++) {
            sessionBuilder.property(in.unpackString(), in.unpackObjectWithType());
        }

        // Session simply tracks active queries. We don't need to store it in resources.
        // Instead, we track active queries in the ClientSession and close them there accordingly.
        Session session = sessionBuilder.build();

        StatementBuilder statementBuilder = sql.statementBuilder()
                .defaultSchema(in.unpackString())
                .pageSize(in.unpackInt())
                .query(in.unpackString())
                .queryTimeout(in.unpackLong(), TimeUnit.MILLISECONDS)
                .prepared(in.unpackBoolean());

        propCount = in.unpackInt();

        for (int i = 0; i < propCount; i++) {
            statementBuilder.property(in.unpackString(), in.unpackObjectWithType());
        }

        Statement statement = statementBuilder.build();

        return session.executeAsync(tx, statement).thenAccept(asyncResultSet -> {
            if (asyncResultSet.hasRowSet() && asyncResultSet.hasMorePages()) {
                try {
                    long resourceId = resources.put(new ClientResource(asyncResultSet, asyncResultSet::close));
                    out.packLong(resourceId);
                } catch (IgniteInternalCheckedException e) {
                    asyncResultSet.close();
                    throw new IgniteInternalException(e.getMessage(), e);
                }
            } else {
                out.packNil(); // resourceId
            }

            out.packBoolean(asyncResultSet.hasRowSet());
            out.packBoolean(asyncResultSet.hasMorePages());
            out.packBoolean(asyncResultSet.wasApplied());

            // Pack metadata.
            if (asyncResultSet.metadata() == null || asyncResultSet.metadata().columns() == null) {
                out.packArrayHeader(0);
            } else {
                List<ColumnMetadata> cols = asyncResultSet.metadata().columns();
                out.packArrayHeader(cols.size());

                for (int i = 0; i < cols.size(); i++) {
                    ColumnMetadata col = cols.get(i);
                    out.packString(col.name());
                    out.packBoolean(col.nullable());

                    // TODO: IGNITE-16962 SQL API: Implement query metadata.
                    // Ideally we only need the type code here.
                    out.packString(col.valueClass().getName());
                    out.packObjectWithType(col.type());
                }
            }

            // Pack first page.
            if (asyncResultSet.hasRowSet()) {
                List<ColumnMetadata> cols = asyncResultSet.metadata().columns();

                out.packInt(asyncResultSet.currentPageSize());

                for (SqlRow row : asyncResultSet.currentPage()) {
                    for (int i = 0; i < cols.size(); i++) {
                        // TODO: IGNITE-16962 pack only the value according to the known type.
                        out.packObjectWithType(row.value(i));
                    }
                }

                if (!asyncResultSet.hasMorePages()) {
                    asyncResultSet.close();
                }
            } else {
                asyncResultSet.close();
            }
        });
    }
}
