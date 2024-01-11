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

package org.apache.ignite.internal.client.table;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.client.ClientUtils.sync;
import static org.apache.ignite.lang.util.IgniteNameUtils.parseSimpleName;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.table.criteria.CursorAdapter;
import org.apache.ignite.internal.table.criteria.SqlSerializer;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.table.criteria.CriteriaQueryOptions;
import org.apache.ignite.table.criteria.CriteriaQuerySource;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for client views.
 */
abstract class AbstractClientView<T> implements CriteriaQuerySource<T> {
    /** Underlying table. */
    protected final ClientTable tbl;

    /**
     * Constructor.
     *
     * @param tbl Underlying table.
     */
    AbstractClientView(ClientTable tbl) {
        assert tbl != null;

        this.tbl = tbl;
    }

    /**
     * Get index mapping from result set to schema.
     *
     * @param columns Columns to map.
     * @param metadata Metadata for query results.
     * @param startInclusive The first index to cover.
     * @param endExclusive Index immediately past the last index to cover.
     * @return Index mapping.
     */
    static List<Integer> indexMapping(
            ClientColumn[] columns,
            int startInclusive,
            int endExclusive,
            @Nullable ResultSetMetadata metadata
    ) {
        if (metadata == null) {
            throw new IgniteException(Sql.RUNTIME_ERR, "Metadata can't be null.");
        }

        return Arrays.stream(columns, startInclusive, endExclusive)
                .map(ClientColumn::name)
                .map(IgniteNameUtils::quoteIfNeeded)
                .map((columnName) -> {
                    var rowIdx = metadata.indexOf(columnName);

                    if (rowIdx == -1) {
                        throw new IgniteException(Sql.RUNTIME_ERR, "Missing required column in query results: " + columnName);
                    }

                    return rowIdx;
                })
                .collect(toList());
    }

    /**
     * Construct SQL query and arguments for prepare statement.
     *
     * @param tableName Table name.
     * @param columns Table columns.
     * @param criteria The predicate to filter entries or {@code null} to return all entries from the underlying table.
     * @return SQL query and it's arguments.
     */
    protected static SqlSerializer createSqlSerializer(String tableName, ClientColumn[] columns, @Nullable Criteria criteria) {
        Set<String> columnNames = Arrays.stream(columns)
                .map(ClientColumn::name)
                .collect(toSet());

        return new SqlSerializer.Builder()
                .tableName(parseSimpleName(tableName))
                .columns(columnNames)
                .where(criteria)
                .build();
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<T> query(@Nullable Transaction tx, @Nullable Criteria criteria, @Nullable CriteriaQueryOptions opts) {
        return new CursorAdapter<>(sync(queryAsync(tx, criteria, opts)));
    }
}
