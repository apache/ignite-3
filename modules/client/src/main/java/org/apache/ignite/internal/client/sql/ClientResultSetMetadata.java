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

import static org.apache.ignite.lang.util.IgniteNameUtils.parseSimpleName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Result set metadata.
 */
class ClientResultSetMetadata implements ResultSetMetadata {
    /** Columns. */
    private final List<ColumnMetadata> columns;

    /** Column name to index map. */
    private final Map<String, Integer> columnIndices;

    /**
     * Constructor.
     *
     * @param unpacker Unpacker.
     */
    public ClientResultSetMetadata(ClientMessageUnpacker unpacker) {
        var size = unpacker.unpackInt();
        assert size > 0 : "ResultSetMetadata should not be empty.";

        var columns = new ArrayList<ColumnMetadata>(size);
        columnIndices =  new HashMap<>(size);

        for (int i = 0; i < size; i++) {
            ClientColumnMetadata column = new ClientColumnMetadata(unpacker, columns);
            columns.add(column);
            columnIndices.put(column.name(), i);
        }

        this.columns = Collections.unmodifiableList(columns);
    }

    /** {@inheritDoc} */
    @Override
    public List<ColumnMetadata> columns() {
        return columns;
    }

    /** {@inheritDoc} */
    @Override
    public int indexOf(String columnName) {
        return columnIndices.getOrDefault(parseSimpleName(columnName), -1);
    }
}
