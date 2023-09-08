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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.util.Cursor;

/**
 * Utility class to work with cursors.
 */
public class CursorUtils {
    /**
     * Read the cursor till the end and collect rows in the list. Order expected to be preserved.
     *
     * @param cur The cursor to read.
     * @return List of rows that were read from the cursor.
     */
    public static <T> List<T> getAllFromCursor(Cursor<T> cur) {
        return StreamSupport.stream(cur.spliterator(), false).collect(Collectors.toList());
    }

    /**
     * Read the cursor till the end and collect rows in the list. Order expected to be preserved.
     *
     * @param cur The cursor to read.
     * @return List of rows that were read from the cursor.
     */
    public static <T> List<T> getAllFromCursor(AsyncCursor<T> cur) {
        List<T> res = new ArrayList<>();
        int batchSize = 256;

        var consumer = new Function<BatchedResult<T>, CompletionStage<BatchedResult<T>>>() {
            @Override
            public CompletionStage<BatchedResult<T>> apply(BatchedResult<T> br) {
                res.addAll(br.items());

                if (br.hasMore()) {
                    return cur.requestNextAsync(batchSize).thenCompose(this);
                }

                return CompletableFuture.completedFuture(br);
            }
        };

        await(cur.requestNextAsync(batchSize).thenCompose(consumer));
        await(cur.closeAsync());

        return res;
    }
}
