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

package org.apache.ignite.client.handler.requests.jdbc;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.jdbc.proto.event.JdbcColumnMeta;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcRequestStatus;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnMetadata.ColumnOrigin;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Client jdbc query metadata request handler.
 */
public class ClientJdbcQueryMetadataRequest {
    /**
     * Processes remote {@code JdbcQueryMetadataRequest}.
     *
     * @param in      Client message unpacker.
     * @param out     Client message packer.
     * @return Operation future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            ClientResourceRegistry resources
    ) throws IgniteInternalCheckedException {
        long cursor = in.unpackLong();

        AsyncSqlCursor<?> cur = resources.get(cursor).get(AsyncSqlCursor.class);

        ResultSetMetadata metadata = cur.metadata();

        if (metadata == null) {
            out.packByte(JdbcRequestStatus.FAILED.getStatus());
            out.packString("Failed to get query metadata for cursor with ID : " + cursor);
            //TODO:IGNITE-15247 A proper JDBC error code should be sent.

            return null;
        }

        List<JdbcColumnMeta> meta = metadata.columns().stream()
                .map(ClientJdbcQueryMetadataRequest::createColumnMetadata)
                .collect(Collectors.toList());

        out.packByte(JdbcRequestStatus.SUCCESS.getStatus());
        new JdbcMetaColumnsResult(meta).writeBinary(out);

        return null;
    }

    /**
     * Create Jdbc representation of column metadata from given origin and RelDataTypeField field.
     *
     * @param fldMeta field metadata contains info about column.
     * @return JdbcColumnMeta object.
     */
    private static JdbcColumnMeta createColumnMetadata(ColumnMetadata fldMeta) {
        ColumnOrigin origin = fldMeta.origin();

        String schemaName = null;
        String tblName = null;
        String colName = null;

        if (origin != null) {
            schemaName = origin.schemaName();
            tblName = origin.tableName();
            colName = origin.columnName();
        }

        return new JdbcColumnMeta(
                fldMeta.name(),
                schemaName,
                tblName,
                colName,
                Commons.columnTypeToClass(fldMeta.type()),
                fldMeta.precision(),
                fldMeta.scale(),
                fldMeta.nullable()
        );
    }
}
