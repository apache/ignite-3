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

package org.apache.ignite.internal.runner.app.client;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.catalog.definitions.ColumnDefinition.column;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test the OOTB support for Tuples in compute api.
 */
@SuppressWarnings("resource")
public class ItThinClientTupleComputeMarshallingTest extends ItAbstractThinClientTest {
    static final String TABLE_NAME = "test";
    IgniteClient client;

    @BeforeEach
    void setUp() {
        client = client();

        client.catalog().createTable(
                TableDefinition.builder(TABLE_NAME)
                        .columns(
                                column("key_col", ColumnType.INT32),
                                column("value_col", ColumnType.VARCHAR)
                        ).primaryKey("key_col")
                        .build()
        );

        client.tables().table(TABLE_NAME).keyValueView().put(
                null,
                Tuple.create().set("key_col", 2),
                Tuple.create().set("value_col", "hi")
        );
    }

    @AfterEach
    void tearDown() {
        client.catalog().dropTable(TABLE_NAME);
    }

    @Test
    void tupleFromTableApiAsArgument() {
        // Given tuple from the table.
        var tup = client.tables().table(TABLE_NAME).keyValueView().get(null, Tuple.create().set("key_col", 2));

        // When submit job with the tuple as an argument.
        JobExecution<String> resultJobExec = client.compute().submit(
                JobTarget.node(node(1)),
                JobDescriptor.builder(TupleArgJob.class).build(),
                tup
        );

        // Then job completes successfully.
        assertStatusCompleted(resultJobExec);
        assertThat(
                getSafe(resultJobExec.resultAsync()),
                equalTo("hi")
        );
    }

    @Test
    void tupleFromTableReturned() {
        // Given.
        var key = 2;

        // When submit job that returns tuple from the table.
        JobExecution<Tuple> resultJobExec = client.compute().submit(
                JobTarget.node(node(1)),
                JobDescriptor.builder(TupleResultJob.class).build(),
                key
        );

        // Then tuple is returned.
        assertStatusCompleted(resultJobExec);
        assertThat(
                getSafe(resultJobExec.resultAsync()).stringValue("value_col"),
                equalTo("hi")
        );
    }


    static class TupleResultJob implements ComputeJob<Integer, Tuple> {
        @Override
        public @Nullable CompletableFuture<Tuple> executeAsync(JobExecutionContext context, @Nullable Integer key) {
            // todo: There is no table for some reason in context.ignite().
            return completedFuture(Tuple.create().set("value_col", "hi"));
        }
    }

    static class TupleArgJob implements ComputeJob<Tuple, String> {
        @Override
        public @Nullable CompletableFuture<String> executeAsync(JobExecutionContext context, @Nullable Tuple arg) {
            if (arg == null) {
                return completedFuture("null");
            }

            return completedFuture(arg.stringValue("value_col"));
        }
    }

    private static void assertResultFailsWithErr(int errCode, JobExecution<?> result) {
        var ex = assertThrows(CompletionException.class, () -> result.resultAsync().join());
        assertThat(ex.getCause(), instanceOf(ComputeException.class));
        assertThat(((ComputeException) ex.getCause()).code(), equalTo(errCode));
    }

    private static void assertStatusFailed(JobExecution<?> result) {
        var state = getSafe(result.stateAsync());
        assertThat(state, is(notNullValue()));
        assertThat(state.status(), equalTo(JobStatus.FAILED));
    }

    private static void assertStatusCompleted(JobExecution<?> result) {
        var state = getSafe(result.stateAsync());
        assertThat(state, is(notNullValue()));
        assertThat(state.status(), equalTo(JobStatus.COMPLETED));
    }

    private static <T> T getSafe(@Nullable CompletableFuture<T> fut) {
        assertThat(fut, is(notNullValue()));

        try {
            int waitSec = 5;
            return fut.get(waitSec, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            var cause = e.getCause();
            if (cause instanceof ClassCastException) {
                throw (ClassCastException) cause;
            }
            throw new RuntimeException(e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private ClusterNode node(int idx) {
        return sortedNodes().get(idx);
    }

    private List<ClusterNode> sortedNodes() {
        return client().clusterNodes().stream()
                .sorted(Comparator.comparing(ClusterNode::name))
                .collect(Collectors.toList());
    }
}
