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

package org.apache.ignite.internal.sql;

import static org.apache.ignite.lang.util.IgniteNameUtils.parseIdentifier;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Results set metadata holder.
 */
public class ResultSetMetadataImpl implements ResultSetMetadata {
    /** Column`s metadata ordered list. */
    private final List<ColumnMetadata> columns;

    /** Column`s metadata map. */
    @IgniteToStringExclude
    private final Object2IntMap<String> columnsIndices;

    /**
     * Constructor.
     *
     * @param columns Columns metadata.
     */
    public ResultSetMetadataImpl(List<ColumnMetadata> columns) {
        this.columns = Collections.unmodifiableList(columns);

        columnsIndices = new Object2IntOpenHashMap<>(columns.size());

        for (int i = 0; i < columns.size(); i++) {
            ColumnMetadata column = columns.get(i);

            columnsIndices.put(SqlCommon.normalizedColumnName(column), i);
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
        return columnsIndices.getOrDefault(parseIdentifier(columnName), -1);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(ResultSetMetadataImpl.class, this);
    }
}
