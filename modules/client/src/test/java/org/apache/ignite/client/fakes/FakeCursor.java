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

package org.apache.ignite.client.fakes;

import static org.apache.ignite.internal.sql.engine.QueryProperty.DEFAULT_SCHEMA;
import static org.apache.ignite.internal.sql.engine.QueryProperty.QUERY_TIMEOUT;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.util.ListToInternalSqlRowAdapter;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Fake {@link AsyncSqlCursor}.
 */
public class FakeCursor implements AsyncSqlCursor<InternalSqlRow> {
    private final Random random;
    private final String qry;
    private final SqlProperties properties;
    private final Object[] params;

    private final List<ColumnMetadata> columns;

    FakeCursor(String qry, SqlProperties properties, Object[] params) {
        this.qry = qry;
        this.properties = properties;
        this.params = params;

        random = new Random();
        columns = new ArrayList<>();

        if ("SELECT PROPS".equals(qry)) {
            columns.add(new FakeColumnMetadata("name", ColumnType.STRING));
            columns.add(new FakeColumnMetadata("val", ColumnType.STRING));
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<BatchedResult<InternalSqlRow>> requestNextAsync(int rows) {
        var batch = new ArrayList<InternalSqlRow>();

        if ("SELECT PROPS".equals(qry)) {
            batch.add(getRow("schema", properties.get(DEFAULT_SCHEMA)));
            batch.add(getRow("timeout", String.valueOf(properties.get(QUERY_TIMEOUT))));
            batch.add(getRow("pageSize", String.valueOf(rows)));
        } else {
            for (int i = 0; i < rows; i++) {
                batch.add(getRow(
                        random.nextInt(), random.nextLong(), random.nextFloat(), random.nextDouble(), UUID.randomUUID().toString(), null));
            }
        }

        return CompletableFuture.completedFuture(new BatchedResult<>(batch, true));
    }

    @Override
    public SqlQueryType queryType() {
        return SqlQueryType.QUERY;
    }

    @Override
    public ResultSetMetadata metadata() {
        return new ResultSetMetadata() {
            @Override
            public List<ColumnMetadata> columns() {
                return columns;
            }

            @Override
            public int indexOf(String columnName) {
                return 0;
            }
        };
    }

    @Override
    public boolean hasNextResult() {
        return false;
    }

    @Override
    public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextResult() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> onClose() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> onFirstPageReady() {
        throw new UnsupportedOperationException();
    }

    private static InternalSqlRow getRow(Object... vals) {
        return new ListToInternalSqlRowAdapter(List.of(vals));
    }
}
