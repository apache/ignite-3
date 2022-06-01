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

package org.apache.ignite.internal.sql.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Results set metadata holder.
 */
public class ResultSetMetadataImpl implements ResultSetMetadata {
    /** Empty metadata holder. */
    public static final ResultSetMetadata NO_METADATA = new ResultSetMetadataImpl(List.of());

    /** Column`s metadata ordered list. */
    private final List<ColumnMetadata> columns;

    /** Column`s metadata map. */
    @IgniteToStringExclude
    private final Map<String, Integer> columnsIndices;

    /**
     * Constructor.
     *
     * @param columns Columns metadata.
     */
    public ResultSetMetadataImpl(List<ColumnMetadata> columns) {
        this.columns = Collections.unmodifiableList(columns);

        columnsIndices = new HashMap<>(columns.size());

        for (ColumnMetadata column : columns) {
            columnsIndices.put(column.name(), column.order());
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

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(ResultSetMetadataImpl.class, this);
    }
}
