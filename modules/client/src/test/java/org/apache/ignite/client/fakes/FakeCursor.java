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

package org.apache.ignite.client.fakes;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.ResultSetMetadata;
import org.apache.ignite.internal.sql.engine.SqlCursor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;

/**
 * Fake {@link SqlCursor}.
 */
public class FakeCursor implements AsyncSqlCursor<List<Object>> {
    private final Random random;

    FakeCursor() {
        random = new Random();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<BatchedResult<List<Object>>> requestNextAsync(int rows) {
        var batch = new ArrayList<List<Object>>();

        for (int i = 0; i < rows; i++) {
            List<Object> row = new ArrayList<>();
            row.add(random.nextInt());
            row.add(random.nextLong());
            row.add(random.nextFloat());
            row.add(random.nextDouble());
            row.add(UUID.randomUUID().toString());
            row.add(null);

            batch.add(row);
        }

        return CompletableFuture.completedFuture(new BatchedResult<>(batch, true));
    }

    @Override
    public SqlQueryType queryType() {
        return SqlQueryType.QUERY;
    }

    @Override
    public ResultSetMetadata metadata() {
        return null;
    }
}
