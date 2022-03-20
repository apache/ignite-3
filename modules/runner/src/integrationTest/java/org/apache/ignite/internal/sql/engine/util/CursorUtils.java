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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.sneakyThrow;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.sql.engine.AsyncCursor;
import org.apache.ignite.internal.sql.engine.AsyncCursor.BatchedResult;
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
    public static List<List<?>> getAllFromCursor(Cursor<List<?>> cur) {
        return StreamSupport.stream(cur.spliterator(), false).collect(Collectors.toList());
    }

    /**
     * Read the cursor till the end and collect rows in the list. Order expected to be preserved.
     *
     * @param cur The cursor to read.
     * @return List of rows that were read from the cursor.
     */
    public static List<List<?>> getAllFromCursor(AsyncCursor<List<?>> cur) {
        List<List<?>> res = new ArrayList<>();
        int batchSize = 256;

        try {
            Consumer<BatchedResult<List<?>>> consumer = new Consumer<>() {
                @Override
                public void accept(BatchedResult<List<?>> br) {
                    res.addAll(br.items());

                    if (br.hasMore()) {
                        cur.requestNext(batchSize).thenAccept(this);
                    }
                }
            };

            cur.requestNext(batchSize).thenAccept(consumer).toCompletableFuture().get(5, TimeUnit.SECONDS);

            cur.close().get(5, TimeUnit.SECONDS);
        } catch (Throwable ex) {
            if (ex instanceof CompletionException) {
                ex = ex.getCause();
            }

            sneakyThrow(ex);
        }

        return res;
    }
}
