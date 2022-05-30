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

package org.apache.ignite.internal.sql.engine.prepare;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Results set metadata holder.
 */
public class ResultSetMetadataImpl implements ResultSetMetadata {
    /** Column`s metadata ordered list. */
    private final List<ColumnMetadata> columns;

    /** Column`s metadata map. */
    private final Map<String, Integer> columnsIndices;

    public ResultSetMetadataImpl(List<ColumnMetadata> columns) {
        this.columns = columns;

        columnsIndices = new HashMap(columns.size());

        for (int i = 0; i < columns.size(); i++) {
            columnsIndices.put(columns.get(i).name(), i);
        }
    }

    /** {@inheritDoc} */
    @Override
    public List<ColumnMetadata> columns() {
        return columns;
    }

    /** {@inheritDoc} */
    @Override
    public int indexOf(String columnName) {
        Integer idx = columnsIndices.get(columnName);

        return idx == null ? -1 : idx;
    }
}
