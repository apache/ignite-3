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

package org.apache.ignite.internal.client.sql;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ColumnTypeConverter;
import org.apache.ignite.internal.sql.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.ColumnMetadataImpl.ColumnOriginImpl;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnMetadata.ColumnOrigin;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Result set metadata.
 */
final class ClientResultSetMetadata {
    static @Nullable ResultSetMetadata read(ClientMessageUnpacker unpacker) {
        var size = unpacker.unpackInt();

        if (size == 0) {
            return null;
        }

        var columns = new ArrayList<ColumnMetadata>(size);

        for (int i = 0; i < size; i++) {
            columns.add(readColumn(unpacker, columns));
        }

        return new ResultSetMetadataImpl(columns);
    }

    private static ColumnMetadata readColumn(ClientMessageUnpacker unpacker, ArrayList<ColumnMetadata> prevColumns) {
        var propCnt = unpacker.unpackInt();

        assert propCnt >= 6;

        String name = unpacker.unpackString();
        boolean nullable = unpacker.unpackBoolean();
        ColumnType type = ColumnTypeConverter.fromIdOrThrow(unpacker.unpackInt());
        int scale = unpacker.unpackInt();
        int precision = unpacker.unpackInt();

        ColumnOrigin origin;

        if (unpacker.unpackBoolean()) {
            assert propCnt >= 9;

            origin = readOrigin(unpacker, name, prevColumns);
        } else {
            origin = null;
        }

        return new ColumnMetadataImpl(name, type, precision, scale, nullable, origin);
    }

    private static ColumnOrigin readOrigin(
            ClientMessageUnpacker unpacker,
            String cursorColumnName,
            List<ColumnMetadata> prevColumns) {
        String columnName = unpacker.tryUnpackNil() ? cursorColumnName : unpacker.unpackString();

        int schemaNameIdx = unpacker.tryUnpackInt(-1);

        //noinspection ConstantConditions
        String schemaName = schemaNameIdx == -1
                ? unpacker.unpackString()
                : prevColumns.get(schemaNameIdx).origin().schemaName();

        int tableNameIdx = unpacker.tryUnpackInt(-1);

        //noinspection ConstantConditions
        String tableName = tableNameIdx == -1
                ? unpacker.unpackString()
                : prevColumns.get(tableNameIdx).origin().tableName();

        return new ColumnOriginImpl(schemaName, tableName, columnName);
    }
}
