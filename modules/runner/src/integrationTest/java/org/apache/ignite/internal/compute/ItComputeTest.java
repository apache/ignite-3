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

package org.apache.ignite.internal.compute;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.sql.engine.util.CursorUtils.getAllFromCursor;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.AbstractClusterIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for Compute functionality.
 */
class ItComputeTest extends AbstractClusterIntegrationTest {
    @Test
    void executesJobLocally() throws Exception {
        IgniteImpl entryNode = node(0);

        String result = entryNode.compute()
                .execute(Set.of(entryNode.node()), ConcatJob.class, "a", 42)
                .get(1, TimeUnit.SECONDS);

        assertThat(result, is("a42"));
    }

    @Test
    void executesJobLocallyByClassName() throws Exception {
        IgniteImpl entryNode = node(0);

        String result = entryNode.compute()
                .<String>execute(Set.of(entryNode.node()), ConcatJob.class.getName(), "a", 42)
                .get(1, TimeUnit.SECONDS);

        assertThat(result, is("a42"));
    }

    @Test
    void executesJobOnRemoteNodes() throws Exception {
        Ignite entryNode = node(0);

        String result = entryNode.compute()
                .execute(Set.of(node(1).node(), node(2).node()), ConcatJob.class, "a", 42)
                .get(1, TimeUnit.SECONDS);

        assertThat(result, is("a42"));
    }

    @Test
    void executesJobByClassNameOnRemoteNodes() throws Exception {
        Ignite entryNode = node(0);

        String result = entryNode.compute()
                .<String>execute(Set.of(node(1).node(), node(2).node()), ConcatJob.class.getName(), "a", 42)
                .get(1, TimeUnit.SECONDS);

        assertThat(result, is("a42"));
    }

    @Test
    void localExecutionActuallyUsesLocalNode() throws Exception {
        IgniteImpl entryNode = node(0);

        String result = entryNode.compute()
                .execute(Set.of(entryNode.node()), GetNodeNameJob.class)
                .get(1, TimeUnit.SECONDS);

        assertThat(result, is(entryNode.name()));
    }

    @Test
    void remoteExecutionActuallyUsesRemoteNode() throws Exception {
        IgniteImpl entryNode = node(0);
        IgniteImpl remoteNode = node(1);

        String result = entryNode.compute()
                .execute(Set.of(remoteNode.node()), GetNodeNameJob.class)
                .get(1, TimeUnit.SECONDS);

        assertThat(result, is(remoteNode.name()));
    }

    @Test
    void executesFailingJobLocally() {
        IgniteImpl entryNode = node(0);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> {
            entryNode.compute()
                    .execute(Set.of(entryNode.node()), FailingJob.class)
                    .get(1, TimeUnit.SECONDS);
        });

        assertThat(ex.getCause(), is(instanceOf(JobException.class)));
        assertThat(ex.getCause().getMessage(), is("Oops"));
        assertThat(ex.getCause().getCause(), is(notNullValue()));
    }

    @Test
    void executesFailingJobOnRemoteNodes() {
        Ignite entryNode = node(0);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> {
            entryNode.compute()
                    .execute(Set.of(node(1).node(), node(2).node()), FailingJob.class)
                    .get(1, TimeUnit.SECONDS);
        });

        assertThat(ex.getCause(), is(instanceOf(JobException.class)));
        assertThat(ex.getCause().getMessage(), is("Oops"));
        assertThat(ex.getCause().getCause(), is(notNullValue()));
    }

    @Test
    void broadcastsJobWithArguments() {
        IgniteImpl entryNode = node(0);

        Map<ClusterNode, CompletableFuture<String>> results = entryNode.compute()
                .broadcast(Set.of(entryNode.node(), node(1).node(), node(2).node()), ConcatJob.class, "a", 42);

        assertThat(results, is(aMapWithSize(3)));
        for (int i = 0; i < 3; i++) {
            ClusterNode node = node(i).node();
            assertThat(results.get(node), willBe("a42"));
        }
    }

    @Test
    void broadcastsJobByClassName() {
        IgniteImpl entryNode = node(0);

        Map<ClusterNode, CompletableFuture<String>> results = entryNode.compute()
                .broadcast(Set.of(entryNode.node(), node(1).node(), node(2).node()), ConcatJob.class.getName(), "a", 42);

        assertThat(results, is(aMapWithSize(3)));
        for (int i = 0; i < 3; i++) {
            ClusterNode node = node(i).node();
            assertThat(results.get(node), willBe("a42"));
        }
    }

    @Test
    void broadcastExecutesJobOnRespectiveNodes() {
        IgniteImpl entryNode = node(0);

        Map<ClusterNode, CompletableFuture<String>> results = entryNode.compute()
                .broadcast(Set.of(entryNode.node(), node(1).node(), node(2).node()), GetNodeNameJob.class);

        assertThat(results, is(aMapWithSize(3)));
        for (int i = 0; i < 3; i++) {
            ClusterNode node = node(i).node();
            assertThat(results.get(node), willBe(equalTo(node.name())));
        }
    }

    @Test
    void broadcastsFailingJob() throws Exception {
        IgniteImpl entryNode = node(0);

        Map<ClusterNode, CompletableFuture<String>> results = entryNode.compute()
                .broadcast(Set.of(entryNode.node(), node(1).node(), node(2).node()), FailingJob.class);

        assertThat(results, is(aMapWithSize(3)));
        for (int i = 0; i < 3; i++) {
            Object result = results.get(node(i).node())
                    .handle((res, ex) -> ex != null ? ex : res)
                    .get(1, TimeUnit.SECONDS);

            assertThat(result, is(instanceOf(CompletionException.class)));
            assertThat(((CompletionException) result).getCause(), is(instanceOf(JobException.class)));

            JobException ex = (JobException) ((CompletionException) result).getCause();
            assertThat(ex.getMessage(), is("Oops"));
            assertThat(ex.getCause(), is(notNullValue()));
        }
    }

    @Test
    void executesColocatedWithTupleKey() throws Exception {
        createTestTableWithOneRow();

        IgniteImpl entryNode = node(0);

        String actualNodeName = entryNode.compute()
                .executeColocated("PUBLIC.test", Tuple.create(Map.of("k", 1)), GetNodeNameJob.class)
                .get(1, TimeUnit.SECONDS);

        assertThat(actualNodeName, in(allNodeNames()));
    }

    private void createTestTableWithOneRow() {
        executeSql("CREATE TABLE test (k int, v int, CONSTRAINT PK PRIMARY KEY (k))");
        executeSql("INSERT INTO test(k, v) VALUES (1, 101)");
    }

    private List<String> allNodeNames() {
        return IntStream.range(0, nodes())
                .mapToObj(this::node)
                .map(Ignite::name)
                .collect(toList());
    }

    @Test
    void executesColocatedByClassNameWithTupleKey() throws Exception {
        createTestTableWithOneRow();

        IgniteImpl entryNode = node(0);

        String actualNodeName = entryNode.compute()
                .<String>executeColocated("PUBLIC.test", Tuple.create(Map.of("k", 1)), GetNodeNameJob.class.getName())
                .get(1, TimeUnit.SECONDS);

        assertThat(actualNodeName, in(allNodeNames()));
    }

    @Test
    void executesColocatedWithMappedKey() throws Exception {
        createTestTableWithOneRow();

        IgniteImpl entryNode = node(0);

        String actualNodeName = entryNode.compute()
                .executeColocated("PUBLIC.test", 1, Mapper.of(Integer.class), GetNodeNameJob.class)
                .get(1, TimeUnit.SECONDS);

        assertThat(actualNodeName, in(allNodeNames()));
    }

    @Test
    void executesColocatedByClassNameWithMappedKey() throws Exception {
        createTestTableWithOneRow();

        IgniteImpl entryNode = node(0);

        String actualNodeName = entryNode.compute()
                .<Integer, String>executeColocated("PUBLIC.test", 1, Mapper.of(Integer.class), GetNodeNameJob.class.getName())
                .get(1, TimeUnit.SECONDS);

        assertThat(actualNodeName, in(allNodeNames()));
    }

    private List<List<?>> executeSql(String sql, Object... args) {
        return getAllFromCursor(
                node(0).queryEngine().query("PUBLIC", sql, args).get(0)
        );
    }

    private static class ConcatJob implements ComputeJob<String> {
        /** {@inheritDoc} */
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            return Arrays.stream(args)
                    .map(Object::toString)
                    .collect(joining());
        }
    }

    private static class GetNodeNameJob implements ComputeJob<String> {
        /** {@inheritDoc} */
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            return context.ignite().name();
        }
    }

    private static class FailingJob implements ComputeJob<String> {
        /** {@inheritDoc} */
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            throw new JobException("Oops", new Exception());
        }
    }

    private static class JobException extends RuntimeException {
        public JobException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
