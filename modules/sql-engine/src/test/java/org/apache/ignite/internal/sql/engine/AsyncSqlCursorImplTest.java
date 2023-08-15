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

package org.apache.ignite.internal.sql.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.api.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.engine.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.sql.engine.exec.AsyncWrapper;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ResultSetMetadata;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link AsyncSqlCursorImpl}.
 */
public class AsyncSqlCursorImplTest {

    private static final ResultSetMetadata RESULT_SET_METADATA = new ResultSetMetadataImpl(Collections.emptyList());

    /** Cursor should trigger commit of implicit transaction (if any) only if data is fully read. */
    @ParameterizedTest
    @MethodSource("transactions")
    public void testTriggerCommitAfterDataIsFullyRead(NoOpTransaction implicitTx) {
        List<Integer> list = List.of(1, 2, 3);

        AsyncSqlCursorImpl<Integer> cursor = new AsyncSqlCursorImpl<>(SqlQueryType.QUERY, RESULT_SET_METADATA, implicitTx,
                new AsyncWrapper<>(CompletableFuture.completedFuture(list.iterator()), Runnable::run));

        int requestRows = 2;
        BatchedResult<Integer> in1 = cursor.requestNextAsync(requestRows).join();
        assertEquals(in1.items(), list.subList(0, requestRows));

        if (implicitTx != null) {
            CompletableFuture<Void> f = implicitTx.commitFuture();
            assertFalse(f.isDone(), "Implicit transaction should have not been committed because there is more data.");
        }

        BatchedResult<Integer> in2 = cursor.requestNextAsync(requestRows).join();
        assertEquals(in2.items(), list.subList(requestRows, list.size()));

        if (implicitTx != null) {
            CompletableFuture<Void> f = implicitTx.commitFuture();
            assertTrue(f.isDone(), "Implicit transaction should been committed because there is no more data");
        }
    }

    /** Exception on read should trigger rollback of implicit transaction, if any. */
    @ParameterizedTest
    @MethodSource("transactions")
    public void testExceptionRollbacksImplicitTx(NoOpTransaction implicitTx) {
        IgniteException err = new IgniteException(Common.INTERNAL_ERR);

        AsyncSqlCursorImpl<Integer> cursor = new AsyncSqlCursorImpl<>(SqlQueryType.QUERY, RESULT_SET_METADATA, implicitTx,
                new AsyncWrapper<>(CompletableFuture.failedFuture(err), Runnable::run));

        CompletionException t = assertThrows(CompletionException.class, () -> cursor.requestNextAsync(1).join());

        if (implicitTx != null) {
            CompletableFuture<Void> f = implicitTx.rollbackFuture();
            assertTrue(f.isDone(), "Implicit transaction should have been rolled back: " + f);
        }

        IgniteException igniteErr = assertInstanceOf(IgniteException.class, t.getCause());
        assertEquals(err.codeAsString(), igniteErr.codeAsString());
    }

    /** Cursor close should trigger commit of implicit transaction, if any. */
    @ParameterizedTest
    @MethodSource("transactions")
    public void testCloseCommitsImplicitTx(NoOpTransaction implicitTx) {
        AsyncCursor<Integer> data = new AsyncWrapper<>(List.of(1, 2, 3, 4).iterator());
        AsyncSqlCursorImpl<Integer> cursor = new AsyncSqlCursorImpl<>(SqlQueryType.QUERY, RESULT_SET_METADATA, implicitTx, data);
        cursor.closeAsync().join();

        if (implicitTx != null) {
            CompletableFuture<Void> f = implicitTx.commitFuture();
            assertTrue(f.isDone(), "Implicit transaction should have been committed: " + f);
        }
    }

    private static Stream<Arguments> transactions() {
        return Stream.of(
                Arguments.of(Named.named("implicit-tx", NoOpTransaction.readOnly("TX"))),
                Arguments.of(Named.named("no implicit-tx", null))
        );
    }
}
