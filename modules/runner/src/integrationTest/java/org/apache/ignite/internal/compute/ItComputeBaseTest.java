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

package org.apache.ignite.internal.compute;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;

/**
 * Base integration tests for Compute functionality.
 */
public abstract class ItComputeBaseTest extends ClusterPerTestIntegrationTest {
    protected abstract List<DeploymentUnit> units();

    protected abstract String concatJobClassName();

    protected abstract String getNodeNameJobClassName();

    protected abstract String failingJobClassName();

    protected abstract String jobExceptionClassName();

    @Test
    void executesJobLocally() {
        IgniteImpl entryNode = node(0);

        String result = entryNode.compute()
                .<String>execute(Set.of(entryNode.node()), units(), concatJobClassName(), "a", 42);

        assertThat(result, is("a42"));
    }

    @Test
    void executesJobLocallyAsync() throws Exception {
        IgniteImpl entryNode = node(0);

        String result = entryNode.compute()
                .<String>executeAsync(Set.of(entryNode.node()), units(), concatJobClassName(), "a", 42)
                .get(1, TimeUnit.SECONDS);

        assertThat(result, is("a42"));
    }

    @Test
    void executesJobOnRemoteNodes() {
        Ignite entryNode = node(0);

        String result = entryNode.compute()
                .execute(Set.of(node(1).node(), node(2).node()), units(), concatJobClassName(), "a", 42);

        assertThat(result, is("a42"));
    }

    @Test
    void executesJobOnRemoteNodesAsync() throws Exception {
        Ignite entryNode = node(0);

        String result = entryNode.compute()
                .<String>executeAsync(Set.of(node(1).node(), node(2).node()), units(), concatJobClassName(), "a", 42)
                .get(1, TimeUnit.SECONDS);

        assertThat(result, is("a42"));
    }

    @Test
    void localExecutionActuallyUsesLocalNode() throws Exception {
        IgniteImpl entryNode = node(0);

        String result = entryNode.compute()
                .<String>executeAsync(Set.of(entryNode.node()), units(), getNodeNameJobClassName())
                .get(1, TimeUnit.SECONDS);

        assertThat(result, is(entryNode.name()));
    }

    @Test
    void remoteExecutionActuallyUsesRemoteNode() throws Exception {
        IgniteImpl entryNode = node(0);
        IgniteImpl remoteNode = node(1);

        String result = entryNode.compute()
                .<String>executeAsync(Set.of(remoteNode.node()), units(), getNodeNameJobClassName())
                .get(1, TimeUnit.SECONDS);

        assertThat(result, is(remoteNode.name()));
    }

    @Test
    void executesFailingJobLocally() {
        IgniteImpl entryNode = node(0);

        IgniteException ex = assertThrows(IgniteException.class, () -> entryNode.compute()
                .execute(Set.of(entryNode.node()), units(), failingJobClassName()));

        assertThat(ex.getCause().getClass().getName(), is(jobExceptionClassName()));
        assertThat(ex.getCause().getMessage(), is("Oops"));
        assertThat(ex.getCause().getCause(), is(notNullValue()));
    }

    @Test
    void executesFailingJobLocallyAsync() {
        IgniteImpl entryNode = node(0);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> entryNode.compute()
                .executeAsync(Set.of(entryNode.node()), units(), failingJobClassName())
                .get(1, TimeUnit.SECONDS));

        assertThat(ex.getCause().getClass().getName(), is(jobExceptionClassName()));
        assertThat(ex.getCause().getMessage(), is("Oops"));
        assertThat(ex.getCause().getCause(), is(notNullValue()));
    }

    @Test
    void executesFailingJobOnRemoteNodes() {
        Ignite entryNode = node(0);

        IgniteException ex = assertThrows(IgniteException.class, () -> entryNode.compute()
                .execute(Set.of(node(1).node(), node(2).node()), units(), failingJobClassName()));

        assertThat(ex.getCause().getClass().getName(), is(jobExceptionClassName()));
        assertThat(ex.getCause().getMessage(), is("Oops"));
        assertThat(ex.getCause().getCause(), is(notNullValue()));
    }

    @Test
    void executesFailingJobOnRemoteNodesAsync() {
        Ignite entryNode = node(0);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> entryNode.compute()
                .executeAsync(Set.of(node(1).node(), node(2).node()), units(), failingJobClassName())
                .get(1, TimeUnit.SECONDS));

        assertThat(ex.getCause().getClass().getName(), is(jobExceptionClassName()));
        assertThat(ex.getCause().getMessage(), is("Oops"));
        assertThat(ex.getCause().getCause(), is(notNullValue()));
    }

    @Test
    void broadcastsJobWithArgumentsAsync() {
        IgniteImpl entryNode = node(0);

        Map<ClusterNode, CompletableFuture<String>> results = entryNode.compute()
                .broadcastAsync(Set.of(entryNode.node(), node(1).node(), node(2).node()), units(), concatJobClassName(), "a", 42);

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
                .broadcastAsync(Set.of(entryNode.node(), node(1).node(), node(2).node()), units(), getNodeNameJobClassName());

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
                .broadcastAsync(Set.of(entryNode.node(), node(1).node(), node(2).node()), units(), failingJobClassName());

        assertThat(results, is(aMapWithSize(3)));
        for (int i = 0; i < 3; i++) {
            Exception result = (Exception) results.get(node(i).node())
                    .handle((res, ex) -> ex != null ? ex : res)
                    .get(1, TimeUnit.SECONDS);

            assertThat(result, is(instanceOf(CompletionException.class)));
            assertThat(result.getCause().getClass().getName(), is(jobExceptionClassName()));
            assertThat(result.getCause().getMessage(), is("Oops"));
            assertThat(result.getCause(), is(notNullValue()));
        }
    }

    @Test
    void executesColocatedWithTupleKey() {
        createTestTableWithOneRow();

        IgniteImpl entryNode = node(0);

        String actualNodeName = entryNode.compute()
                .executeColocated("test", Tuple.create(Map.of("k", 1)), units(), getNodeNameJobClassName());

        assertThat(actualNodeName, in(allNodeNames()));
    }

    @Test
    void executesColocatedWithTupleKeyAsync() throws Exception {
        createTestTableWithOneRow();

        IgniteImpl entryNode = node(0);

        String actualNodeName = entryNode.compute()
                .<String>executeColocatedAsync("test", Tuple.create(Map.of("k", 1)), units(), getNodeNameJobClassName())
                .get(1, TimeUnit.SECONDS);

        assertThat(actualNodeName, in(allNodeNames()));
    }

    @Test
    void executeColocatedThrowsTableNotFoundExceptionWhenTableDoesNotExist() {
        IgniteImpl entryNode = node(0);

        var ex = assertThrows(CompletionException.class,
                () -> entryNode.compute().executeColocatedAsync(
                        "\"bad-table\"", Tuple.create(Map.of("k", 1)), units(), getNodeNameJobClassName()).join());

        assertInstanceOf(TableNotFoundException.class, ex.getCause());
        assertThat(ex.getCause().getMessage(), containsString("The table does not exist [name=\"PUBLIC\".\"bad-table\"]"));
    }

    private void createTestTableWithOneRow() {
        executeSql("CREATE TABLE test (k int, v int, CONSTRAINT PK PRIMARY KEY (k))");
        executeSql("INSERT INTO test(k, v) VALUES (1, 101)");
    }

    private List<String> allNodeNames() {
        return IntStream.range(0, initialNodes())
                .mapToObj(this::node)
                .map(Ignite::name)
                .collect(toList());
    }

    @Test
    void executesColocatedWithMappedKey() {
        createTestTableWithOneRow();

        IgniteImpl entryNode = node(0);

        String actualNodeName = entryNode.compute()
                .executeColocated("test", 1, Mapper.of(Integer.class), units(), getNodeNameJobClassName());

        assertThat(actualNodeName, in(allNodeNames()));
    }

    @Test
    void executesColocatedWithMappedKeyAsync() throws Exception {
        createTestTableWithOneRow();

        IgniteImpl entryNode = node(0);

        String actualNodeName = entryNode.compute()
                .<Integer, String>executeColocatedAsync("test", 1, Mapper.of(Integer.class), units(), getNodeNameJobClassName())
                .get(1, TimeUnit.SECONDS);

        assertThat(actualNodeName, in(allNodeNames()));
    }
}
